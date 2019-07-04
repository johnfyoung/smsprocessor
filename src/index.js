import xml2js from 'xml2js';
import config from './config';
import util from 'util';
import fs from 'fs';
import XmlStream from 'xml-stream';
import mongojs from 'mongojs';
import readline from 'readline'

const db = mongojs('messages', ['sms', 'mms']);

db.on('error', function () {
    console.log('we had an error.');
});

/**
 * Parses an entire XML file into a json object
 * 
 * @param {string} path 
 */
const parseFile = async (path) => {
    const readFile = util.promisify(fs.readFile);
    const parser = xml2js.Parser();
    let json = null;
    await readFile(path).then(async data => {
        await parser.parseString(data, (err, result) => {
            json = result;
        });
    });

    return json;
}

const storeSMS = async sms => {
    await db.sms.update(
        {
            address: sms.$.address,
            date: sms.$.date,
        },
        sms.$,
        { upsert: true }
    )
}

const cleanUp = async () => {
    await db.close();
    readline.clearLine(process.stdout);
    console.log("Done");
}

const parseStream = async (path, handler) => {
    const stream = fs.createReadStream(path);
    const xml = new XmlStream(stream);

    xml.preserve('sms', true);
    xml.collect('subitem');
    let count = 0;
    let mmscount = 0;
    xml.on('endElement: sms', async item => {
        await handler(item);
        count++;
        process.stdout.write(`Processed ${count} records\r`);
    });
    xml.on('end', async () => {
        cleanUp();
        stream.close();
    });
    xml.on('endElement: mms', async mms => {
        if (mmscount === 0) {
            process.stdout.write("\nno more sms...going to mms\n");
            readline.clearLine(process.stdout);
            readline.cursorTo(process.stdout, 0);
        }
        mmscount++;
        readline.clearLine(process.stdout);
        //console.log(mms);
        await handler(mms);
        process.stdout.write(`Processed ${mmscount} mms\r`);
    });
};

//config.sourcefile = "/Users/johny/OneDrive/Apps/SMS\ Backup\ and\ Restore/sms-20190629080249.xml";
//parseStream("/Users/johny/OneDrive/Apps/SMS\ Backup\ and\ Restore/sms-20190629080249.xml");
console.log(typeof config.sourcefile);
console.log(config.sourcefile);
parseStream(config.sourcefile, storeSMS);