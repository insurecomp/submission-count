const fastify = require("../fastify");
const instanceCountService = require("../services/instance-submission-count.service");

class instanceCountRoute {
  constructor() {}

  registerRoutes() {
    fastify.get(
      `/dev/api/downloadData/:type`,
      this.getAllInstanceData.bind(this)
    );
  }

  async getAllInstanceData(request, reply) {
    const { type } = request.params;
    return instanceCountService.getInstanceSubmissionCountData(
      type,
      request,
      reply
    );
  }
}

module.exports = new instanceCountRoute();
