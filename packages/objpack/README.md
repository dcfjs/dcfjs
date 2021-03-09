### obj pack for dcfjs

Basically it's same as [msgpack v5](https://www.npmjs.com/package/msgpack5), with some difference in spec:

1. byte 0xc1 is defined as "undefined"
2. undefine value in object will not be ignored.
