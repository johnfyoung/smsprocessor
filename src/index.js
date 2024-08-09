const xml2js = require("xml2js");
const util = require("util");
const fs = require("fs");
const XmlStream = require("xml-stream");
const { MongoClient } = require("mongodb");
const readline = require("readline");
const dotenv = require("dotenv");
dotenv.config();

const client = new MongoClient(
  process.env.MONGODB_URI || "mongodb://localhost:27017/messages",
  { useUnifiedTopology: true }
);

/**
 * Parses an entire XML file into a json object
 * DEPRECATED
 *
 * @param {string} path
 */
const parseFile = async (path) => {
  const readFile = util.promisify(fs.readFile);
  const parser = xml2js.Parser();
  let json = null;
  await readFile(path).then(async (data) => {
    await parser.parseString(data, (err, result) => {
      json = result;
    });
  });

  return json;
};

const report = (count, mmscount) => {
  process.stdout.write(`Processed sms:${count} mms:${mmscount}\r`);
};

/**
 * DEPRECATED
 */
const storeToDB = (msg, cb) => {
  db.sms.update(
    {
      address: msg.address,
      date: msg.date,
    },
    msg,
    { upsert: true },
    function (err, result) {
      cb(err, result);
    }
  );
};

/**
 * Prepares an SMS for storage, adds an upsert to the bulk database task
 *
 * @param {Object} an sms XML node as JSON from a list of messages. All fields are on sms.$
 */
const storeSMS = (sms, bulk) => {
  sms.$.address = sms.$.address.replaceAll("+1", "");
  sms.$.address =
    sms.$.address.length === 11 && sms.$.address[0] === "1"
      ? sms.$.address.substring(1)
      : sms.$.address;
  sms.$.addresses = [sms.$.address];
  sms.$.msgtype = sms.name;
  bulk
    .find({ address: sms.$.address, date: sms.$.date })
    .upsert()
    .replaceOne(sms.$);
};

/**
 * Prepares an MMS for storage, adds an upsert to the bulk database task
 *
 * @param {Object} an mms XML node as JSON from a list of messages.
 */
const storeMMS = (mms, bulk) => {
  mms.$.address = mms.$.address.replaceAll("+1", "");
  // mms messages are parsed into XML children, not all have JSON content
  mms.$.addresses = mms.addrs.$children.filter(
    (child) => typeof child !== "string"
  );
  mms.$.addresses = mms.$.addresses.map((child) => {
    let address = child.$.address.replaceAll("+1", "");
    address =
      address.length === 11 && address[0] === "1"
        ? address.substring(1)
        : address;
    return address;
  });
  const parts = mms.parts.$children.filter(
    (child) => typeof child !== "string"
  );

  mms.$.body = parts.reduce(
    (body, item) => (item.$.ct === "text/plain" ? body + item.$.text : body),
    ""
  );
  mms.$.msgtype = mms.name;
  bulk
    .find({ address: mms.$.address, date: mms.$.date })
    .upsert()
    .replaceOne(mms.$);
};

const cleanUp = async () => {
  await db.close();
  console.log("Done");
};

const parseStream = async (path, bulk, cb) => {
  const stream = fs.createReadStream(path);
  const xml = new XmlStream(stream);

  xml.preserve("sms", true);
  xml.preserve("mms", true);
  xml.collect("subitem");
  let count = 0;
  let mmscount = 0;
  xml.on("endElement: sms", async (item) => {
    try {
      count++;
      const result = await storeSMS(item, bulk);
      report(count, mmscount);
    } catch (e) {
      console.log("error storing sms: ", e);
      console.log("sms item: ", item);
    }
  });
  xml.on("end", async () => {
    //cleanUp();
    console.log("\rFinished stream");
    //process.stdout.write(`Processed sms:${count} mms:${mmscount}\r`);
    cb();
    stream.close();
  });
  xml.on("endElement: mms", async (mms) => {
    try {
      mmscount++;
      const result = await storeMMS(mms, bulk);
      report(count, mmscount);
    } catch (e) {
      console.log("error storing mms: ", e);
    }
  });
};

(async () => {
  const args = process.argv.slice(2);
  if (args[0]) {
    try {
      await client.connect();
      let db = await client.db("messages");
      const bulk = db.collection("sms").initializeUnorderedBulkOp();

      await parseStream(args[0], bulk, async () => {
        console.log("executing bulk...");
        const start_time = new Date();
        const bulkResult = await bulk.execute();
        const end_time = new Date();
        console.log(
          "Exectued time (minutes): ",
          (end_time - start_time) / 1000 / 60
        );
        console.log("bulk result: ", bulkResult.ok === 1 ? "ok" : "not ok");
        console.log("inserted: ", bulkResult.nInserted);
        console.log("upserted: ", bulkResult.nUpserted);
        console.log("matched: ", bulkResult.nMatched);
        console.log("modified: ", bulkResult.nModified);
        await client.close();
      });
    } catch (e) {
      console.log("Error parsing stream: ", e);
    }
  } else {
    console.log("Usage requires a file path");
  }
})();
