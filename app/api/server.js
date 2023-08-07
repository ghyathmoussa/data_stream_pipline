const express = require('express')
const app = express()
const PORT = process.env.PORT || 4000



app.listen(PORT, (e) => {
    if(!e)
        console.log(`Listening to port: ${PORT}`)
    else
        console.log(`Error during run the server: ${e}`)
})