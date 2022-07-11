// prodigal.js
const fs = require('fs');

// Mock Storage for holding packets before colation
const dataStore = {};
const SEC = 60;
const MILISEC = 1000;

// Start:BST storage for stream
class Node {
  constructor(data) {
    this.data = data;
    this.left = null;
    this.right = null;
  }
}

class BinarySearchTree {

 constructor() {
    this.root = null;
  }

  inorder(node, sortedData = []) {
    if (node !== null) {
      this.inorder(node.left,sortedData);
      sortedData.push(...node.data.payload);
      this.inorder(node.right,sortedData);
    }
    return sortedData;
  }

  // returns root of the tree
  getRootNode() {
    return this.root;
  }

  getSortedData(){
    return this.sortedData;
  }

  insert(data) {
    var newNode = new Node(data);
    if (this.root === null) {
      this.root = newNode;
    } else {
      this.insertNode(this.root, newNode);
    }
  }
  insertNode(node, newNode) {
    if (newNode.data.index < node.data.index) {
      if (node.left === null) {
        node.left = newNode;
      } else {
        this.insertNode(node.left, newNode);
      }
    } else {
      if (node.right === null) {
        node.right = newNode;
      } else {
        this.insertNode(node.right, newNode);
      }
    }
  }
}
const BST = new BinarySearchTree();


// End:BST storage for stream


  // ancillary service 

// converting [HI This Is It] to [2,4,2,2] 
function ancillaryService(payload) {
  return payload[0].split(" ").map((w) => w.length);
}

//packet receiver 
function receiveData(data) {
  console.log('packet received for prid',data.prid);  
  if (!dataStore[data.prid]) {
    dataStore[data.prid] = {
      BST: new BinarySearchTree(),
      timeStamp: new Date().getTime(),
    };
  } 

  // ancillary service called
  data["payload"] = ancillaryService(data.payload);
  
  dataStore[data.prid].BST.insert(data);
}


// service keep checking if any prid reaches the threshold of 1 min
setInterval(() => {
  for (const key in dataStore) {
    if (Object.hasOwnProperty.call(dataStore, key)) {
      const element = dataStore[key];

    // waiting for 60 sec since the first packet arrived related to a particular prid
      if (new Date().getTime() - element.timeStamp >= MILISEC * SEC ) {
        
        //collated data
        let single_data_packet = BST.inorder(element.BST.getRootNode())
        //calling webhook

        console.log(single_data_packet)
        webhook(key,single_data_packet);
        // removing data from data store
        delete dataStore[key];
      }

    }
  }
}, 1 * MILISEC);


function webhook(fileName, data) {
    let logger = fs.createWriteStream(`${fileName}.txt`, {
      flags: "a",
    });
    data.forEach((element) => {
      logger.write(`${element}\n`);
    });
    logger.end();
  }


//dumy data 
const dataPoints = [
  { prid: 1, index: 27, payload: ["AB27 AASSXX axctyri oijseodjfpoasdpodk"], endOfStream: true },
  { prid: 1, index: 9, payload: ["AB9 X new9 axctyri"], endOfStream: false },
  { prid: 1, index: 15, payload: ["AB15 axctyri"], endOfStream: false },
  {
    prid: 1,
    index: 13,
    payload: ["AB13 XYz new13 axctyri"],
    endOfStream: false,
  },
  { prid: 2, index: 27, payload: ["AB27 XYz axctyri ahsdoahsdojaosdjpajsd"], endOfStream: true },
  { prid: 2, index: 9, payload: ["AB9 axctyri"], endOfStream: false },
  { prid: 1, index: 25, payload: ["XYz axctyri ahdiashdoashdoadsaods"], endOfStream: false },
  { prid: 1, index: 7, payload: ["axctyri"], endOfStream: false },
  {
    prid: 2,
    index: 17,
    payload: ["AB17 XYz new17 axctyri"],
    endOfStream: false,
  },
  { prid: 2, index: 10, payload: ["AB10 XYz new10"], endOfStream: false },
  { prid: 1, index: 10, payload: ["AB10 XYz new10"], endOfStream: false },
  { prid: 2, index: 15, payload: ["AB15 axctyri"], endOfStream: false },
  {
    prid: 2,
    index: 13,
    payload: ["AB13 XYz new13 axctyri"],
    endOfStream: false,
  },
  { prid: 2, index: 25, payload: ["ajhdka uashdh XYz new25 axctyri ahsdkjhasdhahd"], endOfStream: false },
  { prid: 1, index: 5, payload: ["i"], endOfStream: false },
  {
    prid: 1,
    index: 17,
    payload: ["AB17 XYz new17 axctyri"],
    endOfStream: false,
  },
  { prid: 2, index: 7, payload: ["PPS XYz new7, axctyri"], endOfStream: false },
  { prid: 2, index: 5, payload: ["axctyri"], endOfStream: false },
];


// mocking receive data 
for (const iterator of dataPoints) {
    receiveData(iterator);
}
