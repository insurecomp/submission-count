const COMMON = {
  EMAIL_MS_URL: "https://2j7w7a8hw7.execute-api.us-east-1.amazonaws.com/",
};

const DATA_DUMP_TABLENAMES = {
  adoptionTable: "AdoptionTable",
  lcmTable: "LcmTable",
  libFundRate: "LibertateFundRate",
  fourthFundRate: "FourthFundRateTable",
  lcTable: "LcTableNew",
  lcmTableClassCode: "LcmTableClassCode",
  territoryGroupTable: "TerritoryGroupTable",
};

const DATA_DUMP_TABLE_FILTERS = [
  {
    name: DATA_DUMP_TABLENAMES.adoptionTable,
    filter: "begins_with(lc_date, :y1) OR begins_with(lc_date, :y2)",
    expressionAttributeValues: {
      ":y1": { S: String(new Date().getFullYear() - 1) } /** previous year */,
      ":y2": { S: String(new Date().getFullYear()) } /** current year */,
    },
    limit: 1000,
  },
  {
    name: DATA_DUMP_TABLENAMES.lcmTable,
    filter: undefined,
    expressionAttributeValues: undefined,
    limit: 1000,
  },
  {
    name: DATA_DUMP_TABLENAMES.libFundRate,
    filter: undefined,
    expressionAttributeValues: undefined,
    limit: 1000,
  },
  {
    name: DATA_DUMP_TABLENAMES.fourthFundRate,
    filter: undefined,
    expressionAttributeValues: undefined,
    limit: 1000,
  },
  {
    name: DATA_DUMP_TABLENAMES.lcTable,
    filter: "begins_with(lc_date, :y1) OR begins_with(lc_date, :y2)",
    expressionAttributeValues: {
      ":y1": { S: String(new Date().getFullYear() - 1) } /** previous year */,
      ":y2": { S: String(new Date().getFullYear()) } /** current year */,
    },
    limit: 1000,
  },
];

module.exports = { COMMON, DATA_DUMP_TABLE_FILTERS, DATA_DUMP_TABLENAMES };
