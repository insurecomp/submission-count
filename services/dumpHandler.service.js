/** Third-party modules */
const axios = require("axios");
const fastify = require("../fastify");

/** Built-in Node.js modules */
const fs = require("fs");
const os = require("os");
const path = require("path");

/** AWS SDK modules */
const { ScanCommand } = require("@aws-sdk/client-dynamodb");
const { unmarshall } = require("@aws-sdk/util-dynamodb");

/** Configuration files */
const {
  DATA_DUMP_TABLE_FILTERS,
  DATA_DUMP_TABLENAMES,
  COMMON,
} = require("../config/config");

/** Utility and helper functions */
const Util = require("../utilities");
const XLSX = require("xlsx");

class DataDumpService {
  async cleanupTempDirectory(tempDir) {
    if (fs.existsSync(tempDir)) {
      fs.readdirSync(tempDir).forEach((file) => {
        const filePath = path.join(tempDir, file);
        try {
          fs.rmSync(filePath, { recursive: true, force: true }); // Delete files
          console.log("Deleted");
        } catch (err) {
          console.error("Error deleting:", filePath, err);
        }
      });
    }
  }

  async createTempDirectory() {
    let tempDir = os.tmpdir();
    console.log("tempDir", tempDir);

    //  Fallback if `os.tmpdir()` is not valid
    if (!tempDir || tempDir.includes("undefined")) {
      tempDir = path.join(__dirname, "temp"); // Use project `temp/` folder
      if (!fs.existsSync(tempDir)) {
        fs.mkdirSync(tempDir, { recursive: true }); // Ensure the folder exists
      }
    }
    return tempDir;
  }

  getLcTableData(file_path, state, lc_date) {
    try {
      const lcTable = Util.getTableDataFromExcel(
        file_path,
        DATA_DUMP_TABLENAMES.lcTable
      );

      return lcTable.filter(
        (data) => data.state === state && data.lc_date === lc_date
      );
    } catch (error) {
      console.log("Error in getLcTableData", error);
      return [];
    }
  }

  getLcmValue(file_path, carrier, state, date) {
    try {
      const lcmTable = Util.getTableDataFromExcel(
        file_path,
        DATA_DUMP_TABLENAMES.lcmTable
      );

      /** get the latest record */
      const record = lcmTable
        .filter(
          (data) =>
            data.id === `${carrier}/${state}` &&
            new Date(data.lcm_date) <= new Date(date)
        )
        .sort((a, b) => new Date(b.lcm_date) - new Date(a.lcm_date));

      return record[0].lcm;
    } catch (error) {
      console.log("Error in getLcmValue", error?.message);
      return "No LCM";
    }
  }

  async getLcmTableClassCode(carrier, state, date) {
    try {
      console.log("processing getLcmTableClassCode");

      let lastEvaluatedKey = null;
      const scanParams = {
        TableName: DATA_DUMP_TABLENAMES.lcmTableClassCode,
        FilterExpression:
          "#carrier = :carrier AND #state = :state AND #date <= :date",
        ExpressionAttributeNames: {
          "#date": "lcm_date",
          "#carrier": "carrier",
          "#state": "state",
        },
        ExpressionAttributeValues: {
          ":date": { S: date },
          ":carrier": { S: carrier },
          ":state": { S: state },
        },
        limit: 100,
        ExclusiveStartKey: lastEvaluatedKey,
      };

      let items = [];
      do {
        const data = await fastify.dynamo.send(new ScanCommand(scanParams));
        items = items.concat(data.Items);
        lastEvaluatedKey = data.LastEvaluatedKey;
      } while (lastEvaluatedKey);

      if (items.length === 0) return {};

      const lcmPreparedArray = {};
      for (let key of items) {
        key = { ...unmarshall(key) };
        const lcm = key.lcm || 0;
        const class_code = key.class_code;
        lcmPreparedArray[class_code] = lcm;
      }

      return lcmPreparedArray;
    } catch (error) {
      console.log("Error in getLcmTableClassCode", error);
      return {};
    }
  }

  async getSignedUrl(file_name, file_size) {
    const body = {
      files: [
        {
          name: file_name,
          size: file_size,
        },
      ],
      instance: "InsureComp",
      includeDownloadUrl: true,
    };

    const res = await axios.post(
      `${COMMON.EMAIL_MS_URL}dev/api/getUploadUrl`,
      body,
      {
        headers: Util.getHeaders(),
      }
    );

    if (res.data.status !== "success")
      throw Error("Failed to retrieve signed url");
    return res.data.data[0];
  }

  async fetchDataAndWriteToExcel() {
    const tempDir = await this.createTempDirectory();

    const file_name = `Data-Dump (${new Date().toJSON().slice(0, 10)}).xlsx`;
    const file_path = path.join(tempDir, file_name);

    const tables = [...DATA_DUMP_TABLE_FILTERS];

    try {
      for (const table of tables) {
        console.log("Fetching data for table:", table.name);
        await this.fetchTableRecords(table, file_path);
      }

      console.log("All records written successfully");

      /** Processing data for Manual Rate */
      await this.fetchManualRateRecordsAndWriteToExcel(file_path);

      return {
        tempDir,
        file_name,
        file_path,
      };
    } catch (error) {
      console.log("Error while writing to Excel:", error);
      throw error;
    }
  }

  async fetchManualRateRecords(file_path) {
    console.log("Fetching manual rate records", file_path);
    const manualRateRecords = [];
    try {
      const adoptionTableData = Util.getTableDataFromExcel(
        file_path,
        DATA_DUMP_TABLENAMES.adoptionTable
      );

      for (const adoptionTableRecord of adoptionTableData) {
        const { state, lc_date, carrier, adoption_date } = adoptionTableRecord;
        console.log(
          "adoptionTableRecord ",
          state,
          lc_date,
          carrier,
          adoption_date
        );

        const lcmForClasscodes = await this.getLcmTableClassCode(
          carrier,
          state,
          adoption_date
        );

        const lcmValue = this.getLcmValue(
          file_path,
          carrier,
          state,
          adoption_date
        );

        // let fundRate = null;
        // if (carrier === "carrier_p") {
        //   fundRate = await this.getFundRate(allRecords, state, date);
        // }
        const lcData = this.getLcTableData(file_path, state, lc_date);

        lcData.map((lc_item) => {
          const lc = lc_item.loss_cost;
          const lcm = lcmForClasscodes[lc_item.class_code] || lcmValue;
          manualRateRecords.push({
            State: state,
            carrier,
            "Adoption Date": adoption_date,
            "Class Code": lc_item.class_code,
            "Lcm Value": lcm,
            "LC Value": lc,
            "Manual Rate": !isNaN(Number(lcm)) ? Number(lc) * Number(lcm) : "",
            lc_date,
            // territory_group
            // fund_rate
          });
        });
      }

      return manualRateRecords;
    } catch (error) {
      console.log("Error in processing manual rate ", error);
      return manualRateRecords;
    }
  }

  async fetchManualRateRecordsAndWriteToExcel(file_path) {
    try {
      const manualRateRecords = await this.fetchManualRateRecords(file_path);

      console.log("Writing manualRateRecords to excel");
      const manualWorkbook = XLSX.readFile(file_path);
      Util.writeToExcel(
        file_path,
        manualRateRecords,
        manualWorkbook,
        "Manual Rate"
      );
    } catch (error) {
      console.log("Error in fetchManualRateRecordsAndWriteToExcel ", error);
    }
  }

  async fetchTableRecords(tableInfo, file_path) {
    try {
      let lastEvaluatedKey = null,
        batchNumber = 1;
      const workbook = fs.existsSync(file_path)
        ? XLSX.readFile(file_path)
        : XLSX.utils.book_new();

      do {
        const scanParams = {
          TableName: tableInfo.name,
          ...(tableInfo.filter ? { FilterExpression: tableInfo.filter } : {}),
          ...(tableInfo.expressionAttributeValues
            ? { ExpressionAttributeValues: tableInfo.expressionAttributeValues }
            : {}),
          Limit: tableInfo.limit,
          ExclusiveStartKey: lastEvaluatedKey,
        };

        const scanResult = await fastify.dynamo.send(
          new ScanCommand(scanParams)
        );

        console.log(
          `Batch ${batchNumber}: Writing ${scanResult.Items.length} records to Excel`
        );

        const records = scanResult.Items.map((data) => unmarshall(data));
        Util.writeToExcel(file_path, records, workbook, tableInfo.name);
        batchNumber++;

        lastEvaluatedKey = scanResult.LastEvaluatedKey;
      } while (lastEvaluatedKey);
    } catch (e) {
      console.log("Error in fetchTableRecords", e);
    }
  }

  async processDataDump() {
    try {
      console.log("processing Data Dump");
      const { file_name, file_path, tempDir } =
        await this.fetchDataAndWriteToExcel();

      await this.sendMail(file_path, file_name);
      await this.cleanupTempDirectory(tempDir);
    } catch (error) {
      console.log("Error in processDataDump", error);
    }
  }

  async sendMail(file_path, file_name) {
    try {
      const { downloadUrl } =
        await this.uploadFileAndGetAttachmentKeyWithDownloadUrl(
          file_path,
          file_name
        );

      console.log("Constructing email body");
      const body = {
        to: ["info.trigger@insurecomp.com"],
        subject: "Data dump",
        html: `<p>
              <p>Hi team,</p>
              <p>The data dump for this week has been generated and is available for download. Please find the link below:</p>
              <p><a href="${downloadUrl}" style="color: #007bff; text-decoration: none; font-weight: bold;">Download Data</a></p>
              <p>Let us know if you have any questions.</p>
              <p>Best regards,<br>
              InsureComp Dev Team
              </p>`,
      };

      await axios.post(`${COMMON.EMAIL_MS_URL}dev/api/sendEmail`, body, {
        headers: Util.getHeaders(),
      });

      console.log("Mail sent!");
    } catch (e) {
      console.log("Error sending mail", e);
    }
  }

  async uploadFileAndGetAttachmentKeyWithDownloadUrl(file_path, file_name) {
    const fileSizeInBytes = fs.statSync(file_path).size;
    const { key, signedUrl, downloadUrl } = await this.getSignedUrl(
      file_name,
      fileSizeInBytes
    );
    console.log("Got signed url");

    await this.uploadFile(signedUrl, file_path);

    return { key, downloadUrl };
  }

  async uploadFile(signedUrl, file_path) {
    try {
      const fileData = await fs.promises.readFile(file_path);

      await axios.put(signedUrl, fileData, {
        headers: Util.getHeadersForExcel(),
      });

      console.log("File is uploaded");
    } catch (error) {
      console.log("error in upload file ", error);
      throw Error(error);
    }
  }

  // async getFundRate(allRecords, state, date) {
  //   const fundRateTable = allRecords.find(
  //     (record) => record.name === DATA_DUMP_TABLENAMES.fourthFundRate
  //   ).records;

  //   const record = fundRateTable.filter(
  //     (data) =>
  //       data.state === state && new Date(data.effective_date) <= new Date(date)
  //   );

  //   return record;
  // }

  // async getMultipleTerritorialFactor(carrier, state, date, lcm) {
  //   const scanParams = {
  //     TableName: DATA_DUMP_TABLENAMES.territoryGroupTable,
  //     KeyConditionExpression: "#state = :state",
  //     FilterExpression: "#date <= :date AND #carrier = :carrier",
  //     ExpressionAttributeNames: {
  //       "#state": "state",
  //       "#date": "effective_date",
  //       "#carrier": "carrier",
  //     },
  //     ExpressionAttributeValues: {
  //       ":state": state,
  //       ":date": date,
  //       ":carrier": carrier,
  //     },
  //   };

  //   const scanResult = await fastify.dynamo.send(
  //     new ScanCommand(scanParams)
  //   );

  //   if (scanResult.Items.length === 0) return lcm;

  //   let greatestDate = null;
  //   for (const item of scanResult.Items) {
  //     if (!greatestDate || item.effective_date > greatestDate) {
  //       greatestDate = item.effective_date;
  //     }
  //   }

  //   let nearestEffectiveSelected = scanResult.Items.filter((item) => {
  //     return item.effective_date == greatestDate;
  //   });
  //   let territoryGroupMapRelativityValue = {};
  //   if (nearestEffectiveSelected.length > 0) {
  //     for (let item of nearestEffectiveSelected) {
  //       territoryGroupMapRelativityValue[item.territory_group] =
  //         item.territorial_relativity * lcm;
  //     }
  //     return territoryGroupMapRelativityValue;
  //   } else return {};
  // }
}

module.exports = new DataDumpService();
