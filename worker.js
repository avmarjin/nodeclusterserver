const {sendData, getNodes} = require('./comm.js')
const child = require('child_process')
const net = require('net');
const fs = require('fs')
const log4js = require("log4js");
const http = require('http');
const express = require('express')
const bodyParser = require("body-parser")
const cors = require('cors')
const { pid } = require('process');
let path = require('path');
const { setInterval } = require('timers');

log4js.configure({
  appenders: { nodecluster: { type: "file", filename: "nodecluster.log" } },
  categories: { default: { appenders: ["nodecluster"], level: "debug" } }
});

const logger = log4js.getLogger("nodecluster");

if (!fs.existsSync("pids")) {
    fs.mkdirSync("pids");
}

let rawdata = fs.readFileSync('cluster_settings.json');
let conf = JSON.parse(rawdata);

console.log(`This worker is pid ${process.pid}`)
console.log(`The parent process is pid ${process.ppid}`);

let counter = 0;
let master_stop = true;
let workers = [];
let worknodes = new Map();
let master_time = Date.now();
let isMaster = false;
let masterNode = 0;
let isMasterChanged = false;
let heartBeat = conf.heartBeat;
let taskInterval = conf.taskInterval;
let globalCount = 0;
let activeCount = 0;

let ipc = net.createServer(function (connect) {
	connect.setEncoding('binary');
	connect.on('error',function(exception){
		console.log('socket error:' + exception);
		connect.end();
	});
	//Client close event
	connect.on('close',function(data){
		// console.log('client closed!');
	});
	connect.on("data",function (data) {
         
        let datacontent = JSON.parse(data)
        switch(datacontent.type) {
            case "task": {
                counter++;
              //  console.log(process.pid,": task #",counter, " :", datacontent);
                logger.debug(process.pid,": task #",counter, " :", datacontent);
                sendData("pids/"+datacontent.source,{
                    'type':'report', 
                    'source': process.pid,
                    'counter': counter});
                master_time = Date.now();
                break;
            }
            case "keepalive": {
                // console.log("Master is live ", datacontent.source, " Date: ", datacontent.time)
                logger.debug("Master is live ", datacontent.source, " Date: ", datacontent.time)
                master_time = datacontent.time;
                masterNode = datacontent.source;
                globalCount = datacontent.globalCount;
                break;
            }
            case "report": {
                // console.log(process.pid, ": ", datacontent);
                worknodes.set(datacontent.source, datacontent.counter)
                logger.debug(process.pid, ": ", datacontent);
                activeCount += 1;
                globalCount += 1;
                break;
            }
            case "status": {
                if(datacontent.status) {
                    status = true
                    workers.push(datacontent.pipe)
                    worknodes.set(datacontent.pipe, 0)
                    // console.log(process.pid, " Active workers:", workers)
                    logger.debug(process.pid, " Active workers:", workers)
                } else {
                    // console.log("Deleting ", datacontent.pipe)
                    logger.debug("Deleting ", datacontent.pipe)
                    workers = workers.filter(function(value, index, arr){ return value != datacontent.pipe;})
                    worknodes.delete(datacontent.pipe)
                    //console.log(workers)
                }
                break;
            }
            case "request": {
                sendData("pids/"+datacontent.source,{'type': 'status', 'status':true, 'pipe': process.pid});
                masterNode = datacontent.source;
                master_time = Date.now();
                break;
            }

            case "delete": {
                worker_close();
                process.exit();
                break;
            }

            default: {

            }
        }
    });
});


ipc.listen(path.join(process.cwd(), 'pids', process.pid.toString()), () => {
    console.log("Worker is listen")
    getNodes().then((nodes) => {
        
        nodes.map((node) => {
            if(node !== process.pid.toString())
                sendData("pids/"+node,{'type': 'status', 'status':true, 'pipe': process.pid});
            else if(nodes.length == 1) {    // Если в каталоге только один свой процесс, то считаем его мастером
                isMaster = true; 
                console.log(process.pid, " self detected as first Master")
                for(i=0;i<conf.nodes;i++) child.fork("worker.js")
            }
        })
    })
});


setInterval(()=>{
    if(!isMaster) {
        logger.debug(process.pid, " Master interval: ", (Date.now()-master_time) )
        if((Date.now()-master_time) > 2500) {
            logger.debug("Master is dead. Start voting")
            getNodes().then((nodes) => {
                // console.log(nodes)
                logger.debug(nodes)
                let inodes = [];
                nodes.map((node) => {
                    inodes.push(parseInt(node))
                })
                logger.debug(inodes)
                logger.debug(Math.min(...inodes))
                if(Math.min(...inodes) === process.pid) {
                    workers = [];
                    worknodes.clear();
                    isMaster = true;
                    logger.debug(process.pid, " change to Master")
                }
            })
        }
    }
  
}, heartBeat*2)

let isRequested = false;
setInterval(() => {
    if(isMaster) {
        if(!isMasterChanged) {  // Блок запускается один раз

            isMasterChanged = true;
            const hostname = '127.0.0.1';
            const port = 8000;
            const app = express()
            const server = http.createServer(app)
            server.listen(port, hostname, () => {
                console.log(`Server running at http://${hostname}:${port}/`);
            }); 
            app.use(cors())
            app.use(bodyParser.json())

            // Запрос нодов кластера
            app.get("/api/1.0/getcluster", (req, res) => {
                
                let anodes = [...worknodes].map(([name, value]) => ({ name, value }));
                // console.log("anodes ", anodes)
                res.status(200).send({
                    resultCode: 1,
                    master: pid,
                    workers: workers,
                    nodes: anodes,
                    activeCount: activeCount,
                    globalCount: globalCount
                })
            })

            // Остановка нода кластера по номеру процесса
            app.delete("/api/1.0/deleteworker/:pid", (req, res) => {
                let pid = req.params.pid;
                console.log("Recieve delete for ",pid)
                sendData("pids/"+pid.toString(), {
                    'type': 'delete',
                    'source': process.pid.toString(),
                    'time': Date.now()})
                workers = workers.filter(function(value, index, arr){ return value != pid});
                worknodes.delete(parseInt(pid));
            
                let anodes = [...worknodes].map(([name, value]) => ({ name, value }));
                // console.log(anodes)
                res.status(200).send({
                    resultCode: 1,
                    workers: workers,
                    nodes: anodes
                })
            })

            // Остановка мастера
            app.delete("/api/1.0/deletemaster", (req, res) => {
                
                res.status(200).send({
                    resultCode: 1,
                    workers: [],
                    nodes: []
                })
                server.close();
                ipc.close();
                process.exit();
            })
        }

        // Единоразвоый запрос активных нодов 
        if(workers.length === 0 && !isRequested) {
            getNodes().then((nodes) => {
                // console.log(nodes)
                logger.debug(nodes)
                nodes.map((node) => {
                    if(node !== process.pid.toString())
                        sendData("pids/"+node,{'type': 'request', 'source': process.pid});
                })
            })
            isRequested = true;
        }

        // Распределение задач
        if(workers.length > 0) {
            logger.debug(workers)
            workers.map(pipe => {
                if(fs.existsSync("pids/"+pipe.toString())) {
                    sendData("pids/"+pipe.toString(), {
                        'type': 'task',
                        'source': process.pid.toString(),
                        'data': Math.random().toString()})
                } else {
                    workers = workers.filter(function(value, index, arr){ return value != pipe;})
                    worknodes.delete(parseInt(pipe))
                    // console.log(workers)
                }
                
            })
        }
    }
}, taskInterval)

// Посылка keepalive
setInterval(() => {
    if(isMaster && workers.length > 0) {
        workers.map(pipe => {
            if(fs.existsSync("pids/"+pipe.toString())) {
            sendData("pids/"+pipe.toString(), {
            'type': 'keepalive',
            'source': process.pid.toString(),
            'globalCount': globalCount,
            'time': Date.now()})
            } else {
                workers = workers.filter(function(value, index, arr){ return value != pipe;})
                worknodes.delete(parseInt(pipe))
                //console.log(workers)
            }
        })
    }
}, heartBeat);

function worker_close() {
    ipc.close();
    process.exit();
}

process.on('SIGTERM', () => {
    console.info('SIGTERM signal received.');
    worker_close() 
  
});

process.on('SIGINT', () => {
    console.info('SIGTERM signal received.');
    worker_close() 
});

process.on('exit', () => {
    if(master_stop) {
        console.log("Exit")
    } else {
        child.fork("worker.js")
    }

})


