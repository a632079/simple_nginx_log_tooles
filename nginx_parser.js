'use strict'
// Import Packages
const path = require('path')
const utils = require('util')
const Parser = require('nginxparser')
const fs = require('fs')

global.Promise = require('bluebird')

// Define
const logDir = path.join('./logs')
const parser = new Parser('$remote_addr - $remote_user [$time_local] "$request" $status $body_bytes_sent "$http_referer" "$http_user_agent"')
const Sequelize = require('sequelize')
const sequelize = new Sequelize('dev', 'root', '', {
    host: 'localhost',
    dialect: 'mysql',
    operatorsAliases: false,
    pool: {
        max: 5,
        min: 0,
        acquire: 30000,
        idle: 10000
    }
})
// Logs Table
const Log = sequelize.define('logs', {
  // DB Module
})


// Init
async function init(dir) {
    try {
        // const config = require(path.join('./config.json'))
        await sequelize.sync()
        const results = await parseLogs(dir)
        // console.log(results)
        // console.log(results.data)
        // Without table category
        const events = []
        for (let host of results.data) {
            if (host.length < 1) continue
            events.push(insert(host))
        }
        await Promise.all(events)
        process.exit()
    } catch (e) {
        console.error(e)
        process.exit(1)
    }
}

// Parse Nginx Logs
async function parseLogs(Path) {
    // Read File names via Sync Method
    const fileNames = fs.readdirSync(Path)
    if (!fileNames || fileNames.length < 1) {
        throw new Error('No Log Found!')
    }

    const events = []
    // Push ParseEvents
    for (let fileName of fileNames) {
        events.push(parseLog(fileName, Path))
    }

    return {
        data: await Promise.all(events),
        name: fileNames
    }
}

async function parseLog(fileName, dir) {
    const file = path.join(dir, fileName)

    // Read File
    const origin = (await utils.promisify(fs.readFile)(file)).toString('utf-8')
    if (!origin) {
        return null
    }

    // Array by row
    const _ = origin.split('\r\n')
    const logs = _.slice(0, _.length - 1)

    // Parse 
    const events = []
    for (let row of logs) {
        events.push(parse(row))
    }
    return Promise.all(events)
}

function parse(row) {
    return new Promise((ok, err) => {
        try {
            parser.parseLine(row, parsed => {
                ok(parsed)
            })
        } catch (e) {
            err(e)
        }
    })
}

// Crate Database Data
async function insert(data) {
    const events = []
    for (let row of data) {
        // Do Object Reflection
        // const save_row = {
        //
        //}
        const save_row = row
        console.log(save_row)
        events.push(Log.create(save_row))
    }
    return Promise.all(events)
}

// Start
sequelize
  .authenticate()
  .then(() => {
    console.log('Connection has been established successfully.');
    init(logDir)
  })
  .catch(err => {
    console.error('Unable to connect to the database:', err);
  })