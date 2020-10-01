const net = require('net');
let path = require('path');
const fs = require('fs')

function sendData(pipepath, data4send) {
    let client= new net.Socket();
    client.setEncoding('binary');
    client.connect(path.join(pipepath),function () {
        
        client.write(JSON.stringify(data4send));
        client.end();
        
    });
    client.on('data',function(data){
        console.log("... "+data);
                //The data can be processed properly after receiving the data here.
        client.end();
    });
    client.on('close',function(){
    //  console.log('Connection closed');
    });
    client.on('error',function(error){
        console.log(process.pid, ' error: ', error);
        client.end();
    });

}

async function getNodes() {
    let pathnodes = path.join("pids");
    
    let readdir = (pathnodes) => {
        return new Promise((resolve, reject) => {
         fs.readdir(pathnodes, (error, files) => {
          error ? reject(error) : resolve(files);
         });
        });
    }
    let nodes = await readdir(pathnodes).then((nodes)=> {
        // console.log("nodes",nodes)
        return nodes;
    }).catch((error) => {
        console.log(error)
    })
    return nodes;
}


module.exports = {sendData, getNodes}