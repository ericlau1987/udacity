/* Global Variables */
let apiKey = 'b5987c106b54c23537da199b9b192b4e';
const zip = document.getElementById('zip').textContent
let baseURL = `https://api.openweathermap.org/data/2.5/weather?zip=${zip},aus&appid=${apiKey}`

console.log(zip)
// Create a new date instance dynamically with JS
let d = new Date();
let newDate = d.getMonth()+'.'+ d.getDate()+'.'+ d.getFullYear();

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