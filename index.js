'use strict';

const Promise = require("es6-promise").Promise;
const winston = require('winston');
const neo4j = require('neo4j-driver').v1;

// At RisingStack, we usually set the configuration from an environment variable called LOG_LEVEL
// winston.level = process.env.LOG_LEVEL
winston.level = 'info';

const username = "neo4j";
const password = "p2b2";
const uri = "bolt://localhost:7687";
var session = null;
var driver = null;

var Neo4jAnalyzer = function () {
};

Neo4jAnalyzer.prototype.connect = () => {
    return new Promise((resolve, reject) => {
        var newDriver = neo4j.driver(uri, neo4j.auth.basic(username, password));
        let newSession = newDriver.session();

        if (newSession._open != true) {
            winston.log('error', 'Neo4jConnector - Driver instantiation failed');
            reject('Driver instantiation failed');
        } else {
            winston.log('info', 'Neo4jConnector - Driver instantiation succeeded');
            driver = newDriver;
            session = newSession;
            resolve(true);
        }

        // TODO: The approach below would be better, but for some reason it does not call the callbacks
        // Register a callback to know if driver creation was successful:
        /*newDriver.onCompleted = () => {
         driver = newDriver;
         session = newSession;
         resolve(newSession);
         };*/
        // Register a callback to know if driver creation failed.
        // This could happen due to wrong credentials or database unavailability:
        /*newDriver.onError = (error) => {
         reject(error);
         };*/
    })
};

Neo4jAnalyzer.prototype.disconnect = () => {
    session.close();
    driver.close();
};

/**
 * Computes the 10 accounts with the highest degree centrality
 * @returns {Promise}
 */
Neo4jAnalyzer.prototype.getAccountDegreeCentrality = () => {
    return new Promise((resolve, reject) => {
        let resultPromise = session.run(
            'match (n:Account)-[r:Transaction]-(m:Account) ' +
            'return n.address, count(r) as DegreeScore ' +
            'order by DegreeScore desc ' +
            'limit 10;'
        );

        Promise.all([resultPromise]).then(promisesResult => {
            resolve(promisesResult[0]);
        }).catch(promisesError => {
            reject(promisesError);
        });
    })
};

/**
 * Computes the 10 external accounts with the highest degree centrality
 * @returns {Promise}
 */
Neo4jAnalyzer.prototype.getExternalDegreeCentrality = () => {
    return new Promise((resolve, reject) => {
        let resultPromise = session.run(
            'match (n:External)-[r:Transaction]-(m:Account) ' +
            'return n.address, count(r) as DegreeScore ' +
            'order by DegreeScore desc ' +
            'limit 10;'
        );

        Promise.all([resultPromise]).then(promisesResult => {
            resolve(promisesResult[0]);
        }).catch(promisesError => {
            reject(promisesError);
        });
    })
};

/**
 * Computes the 10 contracts with the highest degree centrality
 * @returns {Promise}
 */
Neo4jAnalyzer.prototype.getContractDegreeCentrality = () => {
    return new Promise((resolve, reject) => {
        let resultPromise = session.run(
            'match (n:Contract)-[r:Transaction]-(m:Account) ' +
            'return n.address, count(r) as DegreeScore ' +
            'order by DegreeScore desc ' +
            'limit 10;'
        );

        Promise.all([resultPromise]).then(promisesResult => {
            resolve(promisesResult[0]);
        }).catch(promisesError => {
            reject(promisesError);
        });
    })
};


/**
 * Computes the 10 accounts with the highest betweenness centrality
 * Only use on a machine with high memory resources
 * @returns {Promise}
 */
Neo4jAnalyzer.prototype.getAccountBetweennessCentrality = () => {
    return new Promise((resolve, reject) => {
        let resultPromise = session.run(
            'MATCH p=allShortestPaths((source:Account)-[:Transaction*]-(target:Account)) ' +
            'WHERE id(source) < id(target) and length(p) > 1 ' +
            'UNWIND nodes(p)[1..-1] as n ' +
            'RETURN n.address, count(*) as betweenness ' +
            'ORDER BY betweenness DESC'
        );

        Promise.all([resultPromise]).then(promisesResult => {
            resolve(promisesResult[0]);
        }).catch(promisesError => {
            reject(promisesError);
        });
    })
};

/**
 * Computes a graph for a list of provided accounts. The links in the created graph are limited to 300 because of
 * computing power reasons.
 * @returns {Promise}
 */
Neo4jAnalyzer.prototype.getGraphForAccounts = (accounts) => {
    accounts = JSON.parse(accounts);
    return new Promise((resolve, reject) => {

        let graphResultPromises = [];
        for (let i = 0; i < accounts.length; i++) {
            let graphResultPromise = session.run(
                'MATCH (accountOne:Account {address: $address})-[r]-(neighbors)' +
                'RETURN accountOne, neighbors, r LIMIT 40',
                {address: accounts[i].toLowerCase()}
            );
            graphResultPromises.push(graphResultPromise);
        }

        Promise.all(graphResultPromises).then(result => {

            let aggregatedResult = {records: []};
            for (let i = 0; i < result.length; i++) {
                for (let j = 0; j < result[i].records.length; j++) {
                    let singleRecord =  result[i].records[j];
                    aggregatedResult.records.push(singleRecord)
                }
            }

            let graphData = convertGraph(aggregatedResult);
            resolve(graphData);
        }).catch(promisesError => {
            reject(promisesError);
        });
    })
};

Neo4jAnalyzer.prototype.getGraphForAccount = (accountAddress) => {
    return new Promise((resolve, reject) => {
        let nodesResultPromise = session.run(
            'MATCH (accountOne:Account {address: $address})-[r]-(neighbors)' +
            'RETURN accountOne, neighbors, r LIMIT 300',
            {address: accountAddress.toLowerCase()}
        );

        nodesResultPromise.then(result => {
            let graphData = convertGraph(result);
            resolve(graphData);
        }).catch(promisesError => {
            reject(promisesError);
        });
    })
};

let convertGraph = function (neo4jResponse) {
    let addedAccounts = [];
    let convertedNodes = [];
    let convertedLinks = [];

    for (let i = 0; i < neo4jResponse.records.length; i++) {
        let singleRecord = neo4jResponse.records[i];
        let nodeOne = singleRecord._fields[0];
        let nodeTwo = singleRecord._fields[1];
        let link = singleRecord._fields[2];

        if (addedAccounts.indexOf(nodeOne.identity.toString()) === -1) {
            let convertedNode = convertNeo4jNode(nodeOne);
            addedAccounts.push(nodeOne.identity.toString());
            convertedNodes.push(convertedNode);
        }

        if (addedAccounts.indexOf(nodeTwo.identity.toString()) === -1) {
            let convertedNode = convertNeo4jNode(nodeTwo);
            addedAccounts.push(nodeTwo.identity.toString());
            convertedNodes.push(convertedNode);
        }

        let convertedLink = convertNeo4jLink(link);
        if (convertedLink !== null) {
            convertedLinks.push(convertedLink);
        }
    }

    for (let i = 0; i < convertedLinks.length; i++) {
        for (let j = 0; j < convertedNodes.length; j++) {
            if (convertedLinks[i].source === convertedNodes[j].id) convertedLinks[i].source = j;
            if (convertedLinks[i].target === convertedNodes[j].id) convertedLinks[i].target = j;
        }
    }

    return {
        "nodes": convertedNodes,
        "links": convertedLinks
    };
};

let convertNeo4jLink = function (neo4jLink) {
    let convertedLink = null;

    if (neo4jLink.type === "Transaction") {
        neo4jLink.properties = {
            input: neo4jLink.properties.input,
            blockNumber: neo4jLink.properties.blockNumber.toString(),
            gas: neo4jLink.properties.gas.toString(),
            from: neo4jLink.properties.from,
            transactionIndex: neo4jLink.properties.transactionIndex.toString(),
            to: neo4jLink.properties.to,
            value: neo4jLink.properties.value.toString(),
            gasPrice: neo4jLink.properties.gasPrice.toString()
        };
        convertedLink = {
            "id": neo4jLink.identity.toString(),
            "source": neo4jLink.start.toString(),
            "target": neo4jLink.end.toString(),
            "type": neo4jLink.type,
            "properties": neo4jLink.properties
        };
    } else if (neo4jLink.type === "Mined") {
        convertedLink = {
            "id": neo4jLink.identity.toString(),
            "source": neo4jLink.start.toString(),
            "target": neo4jLink.end.toString(),
            "type": neo4jLink.type
        };
    }
    return convertedLink;
}

let convertNeo4jNode = function (neo4jNode) {
    if (neo4jNode.labels.indexOf("External") !== -1) {
        neo4jNode.labels = "External";
    } else if (neo4jNode.labels.indexOf("Contract") !== -1) {
        neo4jNode.labels = "Contract";
    } else if (neo4jNode.labels.indexOf("Block") !== -1) {
        neo4jNode.labels = "Block";
        neo4jNode.properties.blockNumber = neo4jNode.properties.blockNumber.toString()
    } else {
        neo4jNode.labels = neo4jNode.labels[0];
    }
    return {
        "id": neo4jNode.identity.toString(),
        "index": neo4jNode.identity.toString(),
        "label": neo4jNode.labels,
        "properties": neo4jNode.properties
    };
}


module.exports = new Neo4jAnalyzer();