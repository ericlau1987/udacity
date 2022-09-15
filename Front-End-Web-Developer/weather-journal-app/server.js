// Setup empty JS object to act as endpoint for all routes
const projectData = {};
let maxKey = ""

// Require Express to run server and routes
const express = require('express');
// Start up an instance of app
const app = express();
/* Dependencies */
const bodyParser = require('body-parser')
/* Middleware*/
//Here we are configuring express to use body-parser as middle-ware.
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

// Cors for cross origin allowance
const cors = require('cors');
app.use(cors());
// Initialize the main project folder
app.use(express.static('website'));


// Setup Server
const port = 8000;

function listening() {
    console.log(`running on localhost: ${port}`)
}

const server = app.listen(port, listening);

// get method route
app.get('/recent', sendRecentData)

function sendRecentData(request, response) {
    response.send(projectData[maxKey])
    console.log(maxKey)
    console.log(projectData[maxKey])
    console.log("Data is sent")
}

// post method route
app.post('/add', callBack)

function callBack(request, response){
    let data = request.body;
    let key = ""
    // if the projectData is not empty
    // find the highest key number and
    // then add 1 to create a new key
    if(Object.entries(projectData).length != 0) {
        const keys = Object.keys(projectData);
        const numbers = keys.map(function(key){
        return parseInt(key.replace("key", '') || 0);
        })
        const highest = Math.max.apply(null, numbers);
        key = "key" + (highest+1)
    }else {
        key = "key0"
    };

    newEntry = {
        temperature: data.temperature,
        date: data.date,
        user_response: data.feeling
        }
    projectData[key] = newEntry
    maxKey = key
    console.log(projectData)
    // console.log(maxKey)
    console.log("Post received")
    // response.send('Post received')
}