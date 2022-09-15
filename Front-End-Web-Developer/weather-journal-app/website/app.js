/* Global Variables */
let apiKey = 'b5987c106b54c23537da199b9b192b4e';
let baseURL = `https://api.openweathermap.org/data/2.5/weather?zip={zip},au&appid=${apiKey}`

// console.log(baseURL)
// Create a new date instance dynamically with JS
let d = new Date();
let newDate = d.getMonth()+'.'+ d.getDate()+'.'+ d.getFullYear();

// 
document.getElementById('generate').addEventListener('click', performAction);

// the event is to get zip value
function performAction(e){
    const newZip = document.getElementById('zip').value
    const feeling = document.getElementById('feelings').value
    getWeather(baseURL, newZip)
    .then(function(data) {
        // console.log({temperature: data, feeling: feeling, date: newDate})
        postData('/add', {temperature: data, feeling: feeling, date: newDate})
        .then(updateUI())
    })
}

const getWeather = async (baseURL, zip) => {
    baseURL = baseURL.replace('{zip}', zip);
    const res = await fetch(baseURL)
    console.log(baseURL)
    try {
        
        const data = await res.json()
        return data["main"]["temp"];
    } catch(error) {
        console.log("error", error)
    }
}

const postData = async ( url = '', data = {})=>{
    // console.log(data)
      const response = await fetch(url, {
      method: 'POST', // *GET, POST, PUT, DELETE, etc.
      credentials: 'same-origin', // include, *same-origin, omit
      headers: {
          'Content-Type': 'application/json',
      },
      body: JSON.stringify(data), // body data type must match "Content-Type" header        
    });
  
      try {
        const newData = await response.json();
        return newData
      }catch(error) {
      console.log("error", error);
      // appropriately handle the error
      }
  }

const updateUI = async() => {
const request = await fetch('/recent');
try {
    const recentData = await request.json()
    console.log(recentData)
    // console.log(recentData.feeling)
    document.getElementById('date').innerHTML = recentData.date;
    document.getElementById('temp').innerHTML = recentData.temperature;
    document.getElementById('content').innerHTML = recentData.user_response
} catch(error) {
    console.log("error", error)
}
}
