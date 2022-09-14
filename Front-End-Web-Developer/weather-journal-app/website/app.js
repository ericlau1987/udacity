/* Global Variables */
let apiKey = 'b5987c106b54c23537da199b9b192b4e';
let baseURL = `https://api.openweathermap.org/data/2.5/weather?zip={zip},au&appid=${apiKey}`

// console.log(baseURL)
// Create a new date instance dynamically with JS
let d = new Date();
let newDate = d.getMonth()+'.'+ d.getDate()+'.'+ d.getFullYear();

// 
let newData = document.getElementById('generate').addEventListener('click', performAction);

// the event is to get zip value
function performAction(e){
    const newZip = document.getElementById('zip').value
    const feeling = document.getElementById('feelings').value
    const data = getWeather(baseURL, newZip)
    // need to solve how to fix "promise" when getting data from temp
    // check chaining promises
    console.log({temperature: data["main"]["temp"], feeling: feeling, date: newDate})
    return {temperature: data["main"]["temp"], feeling: feeling, date: newDate}


}

const getWeather = async (baseURL, zip) => {
    baseURL = baseURL.replace('{zip}', zip);
    const res = await fetch(baseURL)
    console.log(baseURL)
    try {
        
        const data = await res.json()
        console.log(data["main"]["temp"])
        return data
        // return data["main"]["temp"];
    } catch(error) {
        console.log("error", error)
    }
}

const postData = async ( url = '', data = {})=>{
    console.log(data)
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
        // console.log(newData);
        return newData
      }catch(error) {
      console.log("error", error);
      // appropriately handle the error
      }
  }
postData('/add', {
    temperature: 30,
    date: newDate,
    user_response: "hello mate"
})