import os from "os";
import {Worker} from "worker_threads";

type ContextType = {[key: string]: Cloneable};

type ProcessingFunction<Item extends Cloneable, Context extends ContextType, Result extends Cloneable> = (
    item: Item,
    context: Context & {readonly threadId: number}
) => Result | Promise<Result>;

// The names of the exports from a module that are valid processing functions.
type ProcessFunctionNames<Module, Item extends Cloneable, Context extends ContextType> = {
    [K in keyof Module]: Module[K] extends ProcessingFunction<Item, Context, Cloneable>
        ? K extends string
            ? K
            : never
        : never;
}[keyof Module];

type WorkerDataInput<Item, Context> = {items: Item[]; context: Context} & (
    | {modulePath: string; exportName: string}
    | {script: string}
);

type WorkerDataInWorker<Item, Context extends {[key: string]: unknown}> = {
    items: Item[];
    context: Context;
    startIndex: number;
    endIndex: number;
    modulePath?: string;
    exportName?: string;
    script?: string;
};

type Awaited<T> = T extends PromiseLike<infer U> ? Awaited<U> : T;

// Valid input for the structured clone algorithm
type Cloneable =
    | string
    | number
    | bigint
    | boolean
    | Date
    | RegExp
    | ArrayBuffer
    | ArrayBufferView
    | Cloneable[]
    | ContextType
    | Set<Cloneable>
    | Map<Cloneable, Cloneable>;

// Will be sent to workers with toString() so it can't reference values (including imports) from outside it.
// But it can reference types since toString() sees the transpiled code.
const workerScript = async <Item extends Cloneable, Context extends ContextType, Result extends Cloneable>() => {
    const {parentPort, workerData, threadId}: typeof import("worker_threads") = require("worker_threads");
    if (!parentPort) {
        throw new Error("Must be called from a worker");
    }
    try {
        const {
            items,
            context,
            startIndex,
            endIndex,
            modulePath,
            exportName,
            script
        }: WorkerDataInWorker<Item, Context> = workerData;
        let processData: ProcessingFunction<Item, Context, Result>;
        if (modulePath && exportName) {
            processData = require(modulePath)[exportName];
        } else if (script) {
            processData = eval(script);
        }

        const contextWithId = {
            ...context,
            threadId
        };

        const results = await Promise.all(
            items.slice(startIndex, endIndex).map(async item => {
                parentPort.postMessage({type: "progress"});
                return processData(item, contextWithId);
            })
        );

        parentPort.postMessage({type: "results", results: results});
    } catch (e) {
        parentPort.postMessage({type: "error", error: e});
    }
};

const processInWorker = <Item, Context, Result>(
    workerData: WorkerDataInput<Item, Context> & {startIndex: number; endIndex: number},
    onProgress?: () => void
) => {
    return new Promise<Result[]>((resolve, reject) => {
        const code = `(${workerScript.toString()})()`;
        const worker = new Worker(code, {eval: true, workerData: workerData});

        worker.on("message", msg => {
            if (msg.type === "results") {
                resolve(msg.results);
            }
            if (msg.type === "error") {
                reject(msg.error);
            }
            if (msg.type === "progress" && onProgress) {
                onProgress();
            }
        });
    });
};

const splitIntoThreads = async <Item, Context, Result>(
    workerData: WorkerDataInput<Item, Context>,
    onProgress?: () => void
) => {
    const threads = os.cpus().length - 1;

    const itemsPerWorker = Math.ceil(workerData.items.length / threads);

    const results = await Promise.all(
        Array.from({length: threads}).map((_, i) => {
            const workerDataWithRange = {
                ...workerData,
                startIndex: i * itemsPerWorker,
                endIndex: (i + 1) * itemsPerWorker
            };
            return processInWorker<Item, Context, Result>(workerDataWithRange, onProgress);
        })
    );

    const output = new Map<Item, Result>();
    results.flat().forEach((res, i) => {
        output.set(workerData.items[i], res);
    });

    return output;
};

const getModulePath = (moduleFactory: () => Promise<unknown>) => {
    const match = moduleFactory.toString().match(/require\("(.*)"\)/);
    if (!match) {
        throw new Error("moduleFactory argument must be '() => import(\"./blah\")'");
    }
    return `./src/${match[1]}`;
};

export const runInParallel = <Item extends Cloneable, Context extends ContextType>(
    items: Item[],
    context: Context,
    onProgress?: () => void
) => {
    return {
        async map<Result extends Cloneable>(
            processData: ProcessingFunction<Item, Context, Result>
        ): Promise<Map<Item, Result>> {
            const workerData = {
                items: items,
                context: context,
                script: processData.toString()
            };
            return splitIntoThreads(workerData, onProgress);
        },
        async module<
            Module extends {[name: string]: unknown},
            ExportName extends ProcessFunctionNames<Module, Item, Context>
        >(
            moduleFactory: () => Promise<Module>,
            exportName: ExportName
        ): Promise<Map<Item, Awaited<ReturnType<Module[ExportName]>>>> {
            const workerData = {
                items: items,
                context: context,
                modulePath: getModulePath(moduleFactory),
                exportName: exportName
            };
            return splitIntoThreads(workerData, onProgress);
        }
    };
};
