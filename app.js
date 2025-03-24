const fastify = require("./fastify");
const { fastifySchedule } = require("@fastify/schedule");
const dynamoDBConn = require("./config/dynamodb");
const downloadInstanceData = require("./routes/instance-submission-count.route");
const runnerRoute = require("./routes/runner.route");
const RunnerService = require("./services/runner");

fastify.register(fastifySchedule);
fastify.register(dynamoDBConn);
fastify.register(require("@fastify/cors"), {
  origin: "*", // Allow all origins
});

//Register all your routes here
downloadInstanceData.registerRoutes();
runnerRoute.registerRoutes();

const start = async () => {
  try {
    await fastify.listen({ port: 3000, host: "0.0.0.0" });
    console.log("server listening on 3000");

    await fastify.ready();
    await RunnerService.run();
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
