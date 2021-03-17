/// <reference types="node" />
export interface TempStorage {
    setItem(key: string, buffer: Buffer): void | Promise<void>;
    appendItem(key: string, buffer: Buffer): void | Promise<void>;
    getItem(key: string): Buffer | Promise<Buffer>;
    getAndDeleteItem(key: string): Buffer | Promise<Buffer>;
    deleteItem(key: string): void | Promise<void>;
    generateKey(): string | Promise<string>;
    cleanUp?(): void | Promise<void>;
    cleanAll?(): void | Promise<void>;
    refreshExpired(key: string): void | Promise<void>;
}
export interface MasterTempStorage extends TempStorage {
    getFactory(): (options: {
        workerId: string;
        endpoint: string;
    }) => TempStorage;
}
