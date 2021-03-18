const recast = require('recast');
const astTypes = require('ast-types');
const b = astTypes.builders;
const n = astTypes.namedTypes;
const parser = require('recast/parsers/babel');

function hookCode(ast) {
  const stack = [
    {
      knownIdentifiers: {},
      isFunction: false,
      upValues: {},
      usedUpValues: {},
    },
  ];
  let top = stack[0];

  function recordRequireDeclaration(dec, moduleName, isVar = false) {
    if (isVar) {
      // var can be used in whole function before defined.
      delete top.usedUpValues[dec.name];
    }
    top.knownIdentifiers[dec.name] = moduleName;
  }

  function getIdentifierFromDefinition(dec, isVar = false) {
    if (n.Identifier.check(dec)) {
      if (isVar) {
        // var can be used in whole function before defined.
        delete top.usedUpValues[dec.name];
      }
      top.knownIdentifiers[dec.name] = top.knownIdentifiers[dec.name] || true;
    }
    if (n.ObjectPattern.check(dec)) {
      for (const property of dec.properties) {
        getIdentifierFromDefinition(property.value, isVar);
      }
    }
    if (n.ArrayPattern.check(dec)) {
      for (const elements of dec.elements) {
        getIdentifierFromDefinition(elements, isVar);
      }
    }
    if (n.AssignmentPattern.check(dec)) {
      getIdentifierFromDefinition(dec.left, isVar);
    }
    if (n.RestElement.check(dec)) {
      getIdentifierFromDefinition(dec.argument, isVar);
    }
  }

  function registerUpValue(name) {
    let moduleName = true;
    for (let i = stack.length - 1; i >= 0; i--) {
      const curr = stack[i];
      if (curr.knownIdentifiers[name]) {
        moduleName = curr.knownIdentifiers[name];
        break;
      }
    }

    for (let i = stack.length - 1; i >= 0; i--) {
      const curr = stack[i];
      if (curr.knownIdentifiers[name]) {
        break;
      }
      if (curr.isFunction) {
        curr.usedUpValues[name] = moduleName;
        if (typeof moduleName === 'string') {
          return;
        }
      }
    }
  }

  function pushStack(isFunction) {
    let knownIdentifiers = {};

    if (!isFunction) {
      // copy every identifier.
      Object.assign(knownIdentifiers, top.knownIdentifiers);
    }
    stack.push(
      (top = {
        knownIdentifiers,
        isFunction,
        usedUpValues: isFunction ? {} : top.isFunction,
        upValues: Object.assign({}, top.upValues, top.knownIdentifiers),
      })
    );
  }

  function popStack() {
    const ret = stack.pop();
    top = stack[stack.length - 1] || null;
    return ret;
  }

  function generateEnvProperty(name, value) {
    if (value === true) {
      return b.property('init', b.identifier(name), b.identifier(name));
    }
    const isRelative = /^\.\.?/.test(value);
    if (!isRelative) {
      return b.property(
        'init',
        b.identifier(name),
        b.callExpression(b.identifier('__requireModule'), [b.literal(value)])
      );
    }

    return b.property(
      'init',
      b.identifier(name),
      b.callExpression(b.identifier('__requireModule'), [
        b.callExpression(
          b.memberExpression(b.identifier('require'), b.identifier('resolve')),
          [b.literal(value)]
        ),
      ])
    );
  }

  recast.visit(ast, {
    visitIdentifier(path) {
      this.traverse(path);

      const self = path.value;
      const parent = path.parentPath.value;
      if (n.Property.check(parent) || n.ObjectProperty.check(parent)) {
        if (self === parent.key) {
          return;
        }
      }
      if (n.MemberExpression.check(parent)) {
        if (self === parent.property) {
          return;
        }
      }
      if (top.upValues[self.name]) {
        registerUpValue(self.name);
      }
    },
    visitVariableDeclaration(path) {
      const isVar = path.value.kind === 'var';
      for (const item of path.value.declarations) {
        if (
          n.CallExpression.check(item.init) &&
          n.Identifier.check(item.init.callee) &&
          item.init.callee.name === 'require' &&
          n.Literal.check(item.init.arguments[0])
        ) {
          // This is a require declaration
          recordRequireDeclaration(
            item.id,
            item.init.arguments[0].value,
            isVar
          );
        } else {
          getIdentifierFromDefinition(item.id, isVar);
        }
      }
      this.traverse(path);
    },
    visitBlock(path) {
      pushStack(false);
      this.traverse(path);
      popStack();
    },
    visitCatchClause(path) {
      pushStack(false);
      getIdentifierFromDefinition(path.value.param);
      this.traverse(path);
      popStack();
    },
    visitFunctionDeclaration(path) {
      getIdentifierFromDefinition(path.value.id, true);
      pushStack(true);
      getIdentifierFromDefinition(path.value.id, true);
      for (const item of path.value.params) {
        if (n.AssignmentExpression.check(item)) {
          getIdentifierFromDefinition(item.left);
        } else {
          getIdentifierFromDefinition(item);
        }
      }
      this.traverse(path);
      const { usedUpValues } = popStack();
      path.insertAfter(
        b.expressionStatement(
          b.callExpression(b.identifier('__captureEnv'), [
            path.value.id,
            b.objectExpression(
              Object.keys(usedUpValues).map((v) =>
                generateEnvProperty(v, usedUpValues[v])
              )
            ),
          ])
        )
      );
    },
    visitArrowFunctionExpression(path) {
      pushStack(true);
      for (const item of path.value.params) {
        if (n.AssignmentExpression.check(item)) {
          getIdentifierFromDefinition(item.left);
        } else {
          getIdentifierFromDefinition(item);
        }
      }
      this.traverse(path);
      const { usedUpValues } = popStack();

      if (Object.keys(usedUpValues).length > 0) {
        path.replace(
          b.callExpression(b.identifier('__captureEnv'), [
            path.value,
            b.objectExpression(
              Object.keys(usedUpValues).map((v) =>
                generateEnvProperty(v, usedUpValues[v])
              )
            ),
          ])
        );
      }
    },
    visitFunctionExpression(path) {
      pushStack(true);
      for (const item of path.value.params) {
        getIdentifierFromDefinition(item);
      }
      this.traverse(path);
      const { usedUpValues } = popStack();

      if (Object.keys(usedUpValues).length > 0) {
        path.replace(
          b.callExpression(b.identifier('__captureEnv'), [
            path.value,
            b.objectExpression(
              Object.keys(usedUpValues).map((v) =>
                generateEnvProperty(v, usedUpValues[v])
              )
            ),
          ])
        );
      }
    },
  });
}

function transformCode(code, options = {}) {
  if (/@dcfjs\/capture-env\/noCaptureEnv/.test(code)) {
    return code;
  }

  const ast = recast.parse(code, { parser });

  let hasNoCaptureEnv = false;

  recast.visit(ast, {
    visitComment(path) {
      for (const comment of path.node.comments) {
        if (/@noCaptureEnv/.test(comment.value)) {
          hasNoCaptureEnv = true;
        }
      }
      this.abort();
    },
  });

  if (hasNoCaptureEnv) {
    return code;
  }

  if (options.es6Modules) {
    ast.program.body.unshift(
      b.importDeclaration(
        [
          b.importSpecifier(
            b.identifier('requireModule'),
            b.identifier('__requireModule')
          ),
          b.importSpecifier(
            b.identifier('captureEnv'),
            b.identifier('__captureEnv')
          ),
        ],
        b.stringLiteral('@dcfjs/common')
      )
    );
  } else {
    ast.program.body.unshift(
      b.variableDeclaration('const', [
        b.variableDeclarator(
          b.objectPattern([
            b.propertyPattern(
              b.identifier('requireModule'),
              b.identifier('__requireModule')
            ),
            b.propertyPattern(
              b.identifier('captureEnv'),
              b.identifier('__captureEnv')
            ),
          ]),
          b.callExpression(b.identifier('require'), [
            b.stringLiteral('@dcfjs/common'),
          ])
        ),
      ])
    );
  }

  hookCode(ast);

  return recast.print(ast).code;
}
exports.transformCode = transformCode;
