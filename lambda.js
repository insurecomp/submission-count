const awsLambdaFastify = require("@fastify/aws-lambda");
const app = require("./app");

const proxy = awsLambdaFastify(app);
// or
// const proxy = awsLambdaFastify(app, { binaryMimeTypes: ['application/octet-stream'] })

// exports.handler = proxy;
// or
exports.handler = async (event, context) => {
  try {
    return await proxy(event, context);
  } catch (err) {
    console.error("Error in Lambda handler:", err);
    return {
      statusCode: 500,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        status: "failure",
        message: "Internal Server Error",
        error: err.message,
      }),
    };
  }
};
// or
// exports.handler = (event, context) => proxy(event, context);
// or
// exports.handler = async (event, context) => proxy(event, context);
