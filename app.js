const fastify = require("./fastify");
const dynamoDBConn = require("./config/dynamodb");
const downloadInstanceData = require("./routes/instance-submission-count.route");

fastify.register(dynamoDBConn);
fastify.register(require("@fastify/cors"), {
  origin: "*", // Allow all origins
});

//Register all your routes here
downloadInstanceData.registerRoutes();

const start = async () => {
  try {
    await fastify.listen({ port: 3000 });
    console.log("server listening on 3000");
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
};

if (require.main === module) {
  // When we run the application using node.js require.main will be set as module
  start();
} else {
  // Othe cases, it require.main will be empty
  module.exports = fastify;
}
