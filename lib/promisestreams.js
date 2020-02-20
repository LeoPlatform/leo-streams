'use strict';

const util = require('util');
const { pipeline } = require('stream');
const through = require('through2').obj;

/**
 * Streams objects through a transform function.
 * @param {(obj, push) => [*]} transformFunction Function used to iterate over stream items.
 * The item, and the stream's push function are passed into the function.
 * @param {(push) => [*]} flushFunction Optional function called when the stream finishes.
 * @returns Stream
 */
function throughPromise (transformFunction, flushFunction) {
	return through(
		async function (obj, enc, callback) {
			await transformFunction(obj, obj => this.push(obj));
			callback();
		},
		flushFunction
			? async function (callback) {
				await flushFunction(obj => this.push(obj));
				callback();
			}
			: undefined
	);
}

/**
 * Streams an object without changing it, while adding to a collection.
 * @param {*} alternativeStream If the divertFunction returns false then push it to through, otherwise push it to the alternativeStream.
 * @param {(obj) => *} divertFunction Validates each object. The divertFunction should return true to pass through, and return an object to pass it to the exception stream instead.
 */
function divergeStream (alternativeStream, divertFunction) {
	return throughPromise(async function (obj, push) {
		const divert = await divertFunction(obj);
		if (divert === false) {
			push(obj);
		} else {
			alternativeStream.write(divert);
		}
	});
}

/**
 * Creates a stream and ties the alternativeStream to its lifecycle
 * @param {*} alternativeStream Alternative stream to end when the main stream ends.
 */
function mergeStream (alternativeStream) {
	const alternativePipe = util.promisify(pipeline)(
		alternativeStream,
		throughPromise(async obj => {
			if (!pushFunction) {
				await new Promise(resolve => setTimeout(resolve, 0));
			}
			pushFunction(obj);
		})
	);
	let pushFunction;
	const mainStream = passThrough(
		function (push) {
			if (!pushFunction) {
				pushFunction = push;
			}
		},
		async function (push) {
			pushFunction = push;
			alternativeStream.end();
			await alternativePipe;
		}
	);
	return mainStream;
}

/**
 * Passes objects through, while running the provided reduceFunction, passing the previous and current item.
 * @param {(previous, current) => *} reduceFunction A modified reduce function. Executed for each item in the stream.
 * The returned value will be passed as the previous parameter to the next call to the reduceFunction.
 * @param {*} defaultValue Starting value for the previous.
 * @returns Stream
 */
function streamReduce (reduceFunction, defaultValue) {
	let previous = defaultValue;
	return throughPromise(async (obj, push) => {
		// eslint-disable-next-line require-atomic-updates
		previous = await reduceFunction(previous, obj);
		push(obj);
	});
}

/**
 * Passes objects through, while running the provided sideFunction, passing the object.
 * @param {(obj) => [*]} sideFunction Function used to iterate over stream objects.
 * @param {(push) => [*]} flushFunction Optional function called when the stream finishes.
 * @returns Stream
 */
function passThrough (sideFunction, flushFunction) {
	return through(
		async function (obj, enc, callback) {
			await sideFunction(obj);
			this.push(obj);
			callback();
		},
		flushFunction
			? async function (callback) {
				await flushFunction(obj => this.push(obj));
				callback();
			}
			: undefined
	);
}

module.exports = {
	divergeStream,
	mergeStream,
	passThrough,
	pipelineAsync: util.promisify(pipeline),
	streamReduce,
	throughPromise,
};
