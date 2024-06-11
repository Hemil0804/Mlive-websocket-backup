
//// services/websocket.js
const aedes = require('aedes');
const { MongoClient } = require('mongodb');
const DB_AUTH_URL = process.env.DB_AUTH_URL_SERVER;
const mqttServer = aedes();
const clientSubscriptions = {};
const clientIntervals = {};
let clientsub = {};
// const clientsub = { clientId: { intervalId: null, active: false } };

mqttServer.on('client', function (client) {
    console.log('Client connected:', client.id);
    clientSubscriptions[client.id] = [];
    clientIntervals[client.id] = [];
});

mqttServer.on('clientDisconnect', function (client) {
    console.log('Client disconnected:', client.id);
    delete clientSubscriptions[client.id];
    if (clientIntervals[client.id]) {
        Object.values(clientIntervals[client.id]).forEach(intervalObj => clearInterval(intervalObj.intervalId));
        delete clientIntervals[client.id];
    }
    // if (clientIntervals[client.id]) {
    //     clientIntervals[client.id].forEach(intervalObj => clearInterval(intervalObj.intervalId));
    //     delete clientIntervals[client.id];
    // }
});

mqttServer.on('subscribe', function (subscriptions, client) {
    console.log('Client subscribed:', client.id, subscriptions);
    subscriptions.forEach((subscription) => {
        const topic = subscription.topic.trim();
        if (!clientSubscriptions[client.id].some(sub => sub.topic === topic)) {
            console.log(client.id,'client.id');
            clientSubscriptions[client.id].push({ topic,inital:true});
        }
    });
    console.log('Updated client subscriptions:', clientSubscriptions);
});

// Handle unsubscriptions
// Handle unsubscriptions
mqttServer.on('unsubscribe', function (subscriptions, client) {
    subscriptions.forEach((topic) => {
        const trimmedTopic = topic.trim();
        if (clientSubscriptions[client.id]) {
            const index = clientSubscriptions[client.id].findIndex(sub => sub.topic === trimmedTopic);
            if (index !== -1) {
                clientSubscriptions[client.id].splice(index, 1);

                // Clear the interval associated with this topic

                if (clientIntervals[client.id]) {
                    const intervalIndex = clientIntervals[client.id].findIndex(interval => interval.topic === trimmedTopic);
                    if (intervalIndex !== -1) {
                        clearInterval(clientIntervals[client.id][intervalIndex].intervalId);
                        clientIntervals[client.id].splice(intervalIndex, 1);

                    }
                }
            } else {
                console.log(`Topic ${trimmedTopic} not found for client ${client.id}`);
            }
        }
    });
});



mqttServer.on('publish', async (packet, client) => {
    if (client) {
        console.log('packet.payload', packet.payload.toString());
        try {
            const content = JSON.parse(packet.payload);
            console.log('JSON is valid.', content);

            const mgclient = new MongoClient(DB_AUTH_URL, {});
            await mgclient.connect();

            // for (const clientId in clientSubscriptions) {
            //     for (const subscription of clientSubscriptions[clientId]) {

            Object.keys(clientSubscriptions).forEach(clientId => {
                clientSubscriptions[clientId].forEach(async subscription => {

                    if (subscription.topic.includes(content.topic)) {
                        const db = mgclient.db('marketv2');
                        const collection = db.collection('scripts');
                        if (content.topic === '/topic/watch/' || content.topic === '/app/topic/watch/') {
                            let finalArray = [];
                            let dataArray = [];
                           
                            let lastCheckDate = new Date();
                            const intervalId = setInterval(async () => {
                                    console.log(clientIntervals[client.id],'subscribe');
                                console.log(clientSubscriptions[clientId],'clientSubscriptions[clientId]nnnn');

                                    try {
                                        const idMap = new Map(content.a.map((id, index) => [id, index]));

                                        if(subscription?.inital==true){
                                            const initialResponse = await collection.find({ id: { $in: content.a } }).toArray();
                                            const initialData = initialResponse.map((doc) => processInitialDocument(doc));
                                            finalArray=initialData
                                            delete subscription.inital;
                                        }else{

                                            const { updatedDocuments, currentDate } = await pollForUpdates(collection, lastCheckDate, content);
                                            lastCheckDate = currentDate;
        
                                            if (updatedDocuments.length === 0) {
                                                return;
                                            }
        
                                            
        
                                            if (updatedDocuments.length > 0) {
                                                dataArray = updatedDocuments.map((doc) => processUpdatedDocument(doc)).filter(doc => doc);
                                            } else {
                                                const result = await collection.find({ id: { $in: content.a } }).toArray();
                                                dataArray = result.map((doc) => processInitialDocument(doc));
                                            }
        
                                            dataArray.forEach(obj => convertSiFieldsToInt(obj));
                                            finalArray = dataArray.filter(doc => Object.keys(doc).length > 1);
                                        }
                                        finalArray.sort((a, b) => idMap.get(a.sn) - idMap.get(b.sn));

                                        const responseMessage = JSON.stringify(finalArray);
                                            // console.log(finalArray,'finalArray')
                                        const topicName = content.topic;
                                        mqttResponse(topicName, client, responseMessage);
                                    } catch (error) {
                                        console.error('Error processing documents:', error);
                                    }
                                }, 300);
                            clientIntervals[client.id].push({ topic: subscription.topic, intervalId });
                            
                        } else if (content.topic == '/app/topic/login' || content.topic == '/user/topic/login') {
                            // const collection = db.collection('scripts');
                            const verifyUser = db.collection('userModel');
                            const appconfig = db.collection('appConfig');
                            const pipeline = [
                                {
                                    $match: {
                                        e: { $nin: [null, "", " "] } // Ensure e is not empty or blank
                                    }
                                },{
                                    $group: {
                                        _id: {
                                            e: "$e",
                                            s: "$s"
                                        },
                                        ids: { $push: "$id" }
                                    }
                                },
                                {
                                    $group: {
                                        _id: "$_id.e",
                                        symbols: {
                                            $push: {
                                                k: "$_id.s",
                                                v: "$ids"
                                            }
                                        }
                                    }
                                },
                                {
                                    $project: {
                                        _id: 0,
                                        e: "$_id",
                                        symbols: {
                                            $arrayToObject: {
                                                $map: {
                                                    input: "$symbols",
                                                    as: "symbol",
                                                    in: {
                                                        k: "$$symbol.k",
                                                        v: "$$symbol.v"
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            ];
                            const scriptListResult = await db.collection('scripts').aggregate(pipeline).toArray();
                            const scriptList = {};
                            scriptListResult.forEach(group => {
                                scriptList[group.e] = group.symbols;
                            });

                            var final_array = [];
                            var appConfigData = []
                            // Query MongoDB for data and send it to the client
                            try {
                                const verifyUserData = await verifyUser.findOne({ userName: content.d, password: content.e });
                                //console.log(verifyUserData, 'verifyUserData')
                                if (verifyUserData) {
                                    var userdata = []
                                    const timestamp = verifyUserData.expiryDate;
                                    const date = new Date(timestamp);
                                    const dateString = date.toISOString().split('T')[0];
                                    const subtimestamp = verifyUserData.subscriptionDate;
                                    const subdate = new Date(subtimestamp);
                                    const subdateString = subdate.toISOString().split('T')[0];
                                    const dateOnly = new Date();
                                    const dateOnlyString = dateOnly.toISOString().split('T')[0];
                                    // console.log(verifyUserData.userScriptsByProfileMap,'verifyUserData.userScriptsByProfileMap')
                                    var userscript = []
                                    verifyUserData.userScriptsByProfileMap.forEach((document) => {
                                        if (document) {
                                            // console.log(document)
                                            userscript.push({
                                                sc: document.scripts,
                                                tp: document.type,
                                                nm: document.profileName,
                                            })
                                        }
                                    });
                                    const updateUserData = await verifyUser.updateOne({ userName: content.d },
                                        { $set: { allowedDevices: content.ae } }
                                    );

                                    userdata.push({
                                        a: verifyUserData._id,
                                        b: verifyUserData.numberofscriptperpage,
                                        c: verifyUserData.fontSize,
                                        d: verifyUserData.userName,
                                        f: verifyUserData.firstName,
                                        g: verifyUserData.lastName,
                                        h: verifyUserData.contactNo,
                                        j: verifyUserData.status,
                                        k: verifyUserData.showIndex,
                                        l: verifyUserData.showCNXnifty,
                                        m: verifyUserData.marketColorTheme,
                                        n: verifyUserData.contractColorTheme,
                                        o: verifyUserData.displayFormat,
                                        p: verifyUserData.city,
                                        q: verifyUserData.macId,
                                        r: verifyUserData.subscriptionType,
                                        s: dateString,
                                        t: subdateString,
                                        u: verifyUserData.mobileColumnMap,
                                        v: userscript,
                                        w: verifyUserData.assignedSegment,
                                        z: verifyUserData.paginationType,
                                        ab: verifyUserData.tradeAllowed,
                                        ac: new Date(),
                                        ad: dateOnlyString,
                                        ae: verifyUserData.allowedDevices,
                                        af: verifyUserData.deviceType
                                    })
                                    //console.log(userdata, 'userdata')

                                    const result = await appconfig.find().toArray()
                                    result.forEach((document) => {
                                        if (document) {
                                            appConfigData.push({
                                                dc1: document.dc1,
                                                dc2: document.dc2,
                                            })
                                        }
                                        });
                                    final_array.push({
                                        code: 1,
                                        data: { d: appConfigData[0], p: scriptList, u: userdata[0] }

                                    })
                                    // Send response back to the client
                                    const responseMessage = JSON.stringify(final_array[0])
                                    var topicName = content.topic
                                    mqttResponse(topicName, client, responseMessage)
                                } else {

                                    // Send response back to the client
                                    const responseMessage = JSON.stringify({ code: 0, messages: "Something Went wrong, password does not match" })
                                    var topicName = content.topic
                                    mqttResponse(topicName, client, responseMessage)
                                }
                                // Now you can work with verifyUserData
                            } catch (error) {
                                console.error(error, 'error');

                                var responseMessage = JSON.stringify({
                                    code: 0,
                                    messages: error
                                })

                                var topicName = content.topic
                                mqttErrorResponse(topicName, client, responseMessage)

                            }
                        } else if (content.topic == '/user/topic/profile/add' || content.topic == '/app/topic/profile/add/') {
                            const collection = db.collection('userModel');
                            try {
                                const checkProfileName = await collection.find({ "userScriptsByProfileMap.profileName": content.nm, userName: content.userName }).toArray();
                                const checkProfilelimit = await collection.find({ userName: content.userName }).toArray();
                                // console.log(checkProfileName, 'checkProfileName')
                                if (checkProfilelimit.length < 9 || checkProfilelimit.length == 9) {

                                    if (checkProfileName.length > 0) {

                                        // Send response back to the client
                                        const responseMessage = JSON.stringify({ code: 0, messages: "Profile Name Already exists" })
                                        var topicName = content.topic
                                        mqttResponse(topicName, client, responseMessage)
                                    } else {
                                        if (content.tp == "Default") {
                                            const checkProfileDefault = await collection.find({ "userScriptsByProfileMap.type": content.tp, userName: content.userName }).toArray();
                                            if (checkProfileDefault.length > 0) {
                                                // Send response back to the client
                                                const responseMessage = JSON.stringify({ code: 0, messages: "You can not set this profile as Default because there is another profile set as default" })
                                                var topicName = content.topic
                                                mqttResponse(topicName, client, responseMessage)
                                            } else {
                                                const result = await collection.updateOne(
                                                    { "userName": content.userName }, // Filter to match the document
                                                    {
                                                        "$push": {
                                                            "userScriptsByProfileMap": {
                                                                "scripts": content.sc,
                                                                "type": content.tp,
                                                                "profileName": content.nm
                                                            }
                                                        }
                                                    }
                                                )
                                                // Send response back to the client
                                                const responseMessage = JSON.stringify({ code: 1, })
                                                var topicName = content.topic
                                                mqttResponse(topicName, client, responseMessage)
                                            }
                                        } else {
                                            const result = await collection.updateOne(
                                                { "userName": content.userName }, // Filter to match the document
                                                {
                                                    "$push": {
                                                        "userScriptsByProfileMap": {
                                                            "scripts": content.sc,
                                                            "type": content.tp,
                                                            "profileName": content.nm
                                                        }
                                                    }
                                                }
                                            )
                                            // Send response back to the client
                                            const responseMessage = JSON.stringify({ code: 1 })
                                            var topicName = content.topic
                                            mqttResponse(topicName, client, responseMessage)
                                        }
                                    }
                                } else {
                                    // Send response back to the client
                                    const responseMessage = JSON.stringify({ code: 0, messages: "You have reached the limit of add profile." })
                                    var topicName = content.topic
                                    mqttResponse(topicName, client, responseMessage)
                                }
                            } catch (error) {
                                console.error(error);

                                var responseMessage = JSON.stringify({
                                    code: 0,
                                    messages: error
                                })

                                var topicName = content.topic
                                mqttErrorResponse(topicName, client, responseMessage)

                            }
                        } else if (content.topic == '/user/topic/profile/delete' || content.topic == '/app/topic/profile/delete/') {
                            const collection = db.collection('userModel');
                            try {
                                const checkProfileName = await collection.find({ "userScriptsByProfileMap.profileName": content.nm, userName: content.userName }).toArray();
                                if (checkProfileName.length > 0) {
                                    const trimmedTopic = content.nm.trim();
                                    const index = checkProfileName[0].userScriptsByProfileMap.findIndex(sub => sub.profileName.trim() === trimmedTopic);
                                    console.log(trimmedTopic, 'topic');
                                    console.log(index, 'index');
                                    console.log(checkProfileName[0].userScriptsByProfileMap, 'checkProfileName[0].userScriptsByProfileMap');

                                    // Check if the topic is found in the client's subscription list
                                    if (index !== -1) {
                                        // Remove the topic from the subscription list
                                        checkProfileName[0].userScriptsByProfileMap.splice(index, 1);
                                        // console.log(checkProfileName[0].userScriptsByProfileMap, 'updated');
                                        const result = await collection.updateOne(
                                            { "userName": content.userName }, // Filter to match the document
                                            { $set: { userScriptsByProfileMap: checkProfileName[0].userScriptsByProfileMap } }
                                        )

                                        if (result.modifiedCount === 1) {
                                            // Send response back to the client
                                            const responseMessage = JSON.stringify({ code: 1, messages: "Profile deleted successfully !" })
                                            var topicName = content.topic
                                            mqttResponse(topicName, client, responseMessage)
                                        }
                                    } else {
                                        // Send response back to the client
                                        const responseMessage = JSON.stringify({ code: 0, messages: "No Profile Name found as per your request" })
                                        var topicName = content.topic
                                        mqttResponse(topicName, client, responseMessage)
                                    }
                                } else {

                                    // Send response back to the client
                                    const responseMessage = JSON.stringify({ code: 0, messages: "No Profile Name found as per your request" })
                                    var topicName = content.topic
                                    mqttResponse(topicName, client, responseMessage)
                                }
                            } catch (error) {
                                const responseMessage = JSON.stringify({ code: 0, messages: error })
                                var topicName = content.topic
                                mqttErrorResponse(topicName, client, responseMessage)
                            }

                        } else if (content.topic == '/user/topic/profile/setdefault' || content.topic == '/app/topic/profile/setdefault/') {
                            const collection = db.collection('userModel');
                            try {
                                const checkProfileName = await collection.find({ "userScriptsByProfileMap.profileName": content.nm, userName: content.userName }).toArray();
                                if (checkProfileName.length > 0) {
                                    collection.updateMany(
                                        { userName: content.userName, "userScriptsByProfileMap.type": "Default" },
                                        { $set: { "userScriptsByProfileMap.$.type": "" } },
                                        function (err, result) {
                                            if (err) {
                                                console.error('Error occurred while updating documents:', err);
                                                return;
                                            }


                                        }
                                    );

                                    const updatedUser = await collection.updateOne(
                                        { userName: content.userName, "userScriptsByProfileMap.profileName": content.nm },
                                        { $set: { "userScriptsByProfileMap.$.type": "Default" } },
                                    );
                                    if (updatedUser.modifiedCount === 1) {
                                        // Send response back to the client
                                        const responseMessage = JSON.stringify({ code: 1, messages: "Profile updated successfully!" })
                                        var topicName = content.topic
                                        mqttResponse(topicName, client, responseMessage)
                                    } else {
                                        // Send response back to the client
                                        const responseMessage = JSON.stringify({ code: 0, messages: "Profile Not updated!" })
                                        var topicName = content.topic
                                        mqttResponse(topicName, client, responseMessage)
                                    }
                                } else {
                                    console.log('asdasd')
                                    // Send response back to the client
                                    const responseMessage = JSON.stringify({ code: 0, messages: "Profile Not Found!" })
                                    var topicName = content.topic
                                    mqttResponse(topicName, client, responseMessage)
                                }
                            } catch (error) {
                                console.log(error, 'error')
                                // Send response back to the client
                                const responseMessage = JSON.stringify({ code: 0, messages: error })
                                var topicName = content.topic
                                mqttErrorResponse(topicName, client, responseMessage)
                            }

                        } else if (content.topic == '/user/topic/profile/addscript' || content.topic == '/app/topic/profile/addscript/') {
                            const collection = db.collection('userModel');
                            try {
                                var result = await collection.updateOne(
                                    {
                                        "userName": content.userName
                                    },
                                    {
                                        "$addToSet": {
                                            "userScriptsByProfileMap.$[profile].scripts": {
                                                "$each": content.sc
                                            }
                                        }
                                    },
                                    {
                                        "arrayFilters": [
                                            {
                                                "profile.profileName": content.nm
                                            }
                                        ]
                                    }
                                )
                                // console.log(result, 'result')
                                if (result.modifiedCount === 1) {
                                    // Send response back to the client
                                    const responseMessage = JSON.stringify({ code: 1, messages: "Script Added Successfully!" })
                                    var topicName = content.topic
                                    mqttResponse(topicName, client, responseMessage)
                                } else {
                                    // Send response back to the client
                                    const responseMessage = JSON.stringify({ code: 1, messages: "Script Already Added!" })
                                    var topicName = content.topic
                                    mqttResponse(topicName, client, responseMessage)
                                }

                            } catch (error) {
                                // Send response back to the client
                                const responseMessage = JSON.stringify({ code: 0, messages: error })
                                var topicName = content.topic
                                mqttErrorResponse(topicName, client, responseMessage)

                            }

                        } else if (content.topic == '/user/topic/profile/removescript' || content.topic == '/app/topic/profile/removescript/') {
                            try {
                                const collection = db.collection('userModel');


                                var result = await collection.updateOne(
                                    {
                                        "userName": content.userName,
                                        "userScriptsByProfileMap.profileName": content.nm,
                                    },
                                    {
                                        $pull: {
                                            "userScriptsByProfileMap.$.scripts": { $in: content.sc }
                                        }
                                    }
                                )
                                console.log(result, 'result')
                                if (result.modifiedCount === 1) {
                                    // Send response back to the client
                                    const responseMessage = JSON.stringify({ code: 1, messages: "Script removed successfully !" })
                                    var topicName = content.topic
                                    mqttResponse(topicName, client, responseMessage)
                                } else {
                                    // Send response back to the client
                                    const responseMessage = JSON.stringify({ code: 0, messages: "Script Not removed " })
                                    var topicName = content.topic
                                    mqttResponse(topicName, client, responseMessage)
                                }
                            } catch (error) {
                                // Send response back to the client
                                const responseMessage = JSON.stringify({ code: 0, messages: error })
                                var topicName = content.topic
                                mqttErrorResponse(topicName, client, responseMessage)
                            }

                        } else if (content.topic == '/user/topic/profile/updateitems' || content.topic == '/app/topic/profile/updateitems/') {
                            const collection = db.collection('userModel');
                            try {
                                if (content.tp == "Default") {
                                    collection.updateMany(
                                        { userName: content.userName, "userScriptsByProfileMap.type": "Default" },
                                        { $set: { "userScriptsByProfileMap.$.type": "" } },
                                        function (err, result) {
                                            if (err) {
                                                console.error('Error occurred while updating documents:', err);
                                                return;
                                            }


                                        }
                                    );

                                    const updatedUser = await collection.updateOne(
                                        { userName: content.userName, "userScriptsByProfileMap.profileName": content.nm },
                                        {
                                            $set: {
                                                "userScriptsByProfileMap.$.type": content.tp,
                                                "userScriptsByProfileMap.$.scripts": content.sc,
                                            }
                                        },
                                    );
                                    if (updatedUser.modifiedCount === 1) {
                                        // Send response back to the client
                                        const responseMessage = JSON.stringify({ code: 1, messages: "Profile updated successfully!" })
                                        var topicName = content.topic
                                        mqttResponse(topicName, client, responseMessage)
                                    } else {
                                        // Send response back to the client
                                        const responseMessage = JSON.stringify({ code: 0, messages: "Profile Not updated!" })
                                        var topicName = content.topic
                                        mqttResponse(topicName, client, responseMessage)
                                    }

                                } else {
                                    const updatedUser = await collection.updateOne(
                                        { userName: content.userName, "userScriptsByProfileMap.profileName": content.nm },
                                        {
                                            $set: {
                                                "userScriptsByProfileMap.$.scripts": content.sc,
                                            }
                                        },
                                    );
                                    if (updatedUser.modifiedCount === 1) {
                                        // Send response back to the client
                                        const responseMessage = JSON.stringify({ code: 1, messages: "Profile updated successfully!" })
                                        var topicName = content.topic
                                        mqttResponse(topicName, client, responseMessage)
                                    } else {
                                        // Send response back to the client
                                        const responseMessage = JSON.stringify({ code: 0, messages: "Profile Not updated!" })
                                        var topicName = content.topic
                                        mqttResponse(topicName, client, responseMessage)
                                    }
                                }
                            } catch (error) {
                                // Send response back to the client
                                const responseMessage = JSON.stringify({ code: 0, messages: error })
                                var topicName = content.topic
                                mqttErrorResponse(topicName, client, responseMessage)
                            }

                        } else if (content.topic == '/user/topic/getholidays' || content.topic == '/app/topic/getholidays') {
                            const collection = db.collection('holiday');

                            var final_array = [];
                            const result = await collection.find();
                            // var bbbbb=result.map(holiday => ({
                            await result.forEach((document) => {
                                if (document) {
                                    final_array.push({
                                        b: document.month,
                                        c: document.holidayData.map(data => ({
                                            a: data.name,
                                            b: data.day,
                                            c: data.type,
                                            d: data.date.toISOString().split('T')[0] // Convert date to ISO string and extract only the date part
                                        }))
                                    })
                                }
                            })
                            const output = {
                                code: 1,
                                data: {
                                    d: final_array
                                }
                            };
                            const responseMessage = JSON.stringify(output)
                            var topicName = content.topic
                            mqttResponse(topicName, client, responseMessage)


                        } else if (content.topic === '/topic/index/' || content.topic === '/app/topic/index/') {
                            const intervalId = setInterval(async () => {
                                try {
                                    const finalArray = [];
                                    const sensex = [];
                                    const nifty = [];
                                    const result = await collection.find({ s: { $in: ["SENSEX", "NIFTY"] }, ix: "11/11/2050" }).toArray();
                                    result.forEach((document) => {
                                        if (document) {
                                            if (document.s === "NIFTY") {
                                                processIndexDocument(document);
                                                nifty.push(document);
                                            } else if (document.s === "SENSEX") {
                                                processIndexDocument(document);
                                                sensex.push(document);
                                            }
                                        }
                                    });
                                    finalArray.push({ nifty: nifty[0], sensex: sensex[0] });
                                    const responseMessage = finalArray.length > 0 ? JSON.stringify(finalArray[0]) : JSON.stringify({ code: 0, messages: "No Index Data Found!" });
                                    const topicName = content.topic;
                                    mqttResponse(topicName, client, responseMessage);
                                } catch (error) {
                                    console.error('Error processing documents:', error);
                                }
                            }, 300);

                            
                            clientIntervals[client.id].push({ topic: subscription.topic, intervalId });
                        } else if (content.topic == '/user/topic/setfont' || content.topic == '/app/topic/setfont/') {
                            const collection = db.collection('userModel');
                            try {
                                const result = await collection.updateOne(
                                    { userName: content.userName },
                                    { $set: { fontSize: content.c } }
                                );
                                // console.log(result,'result')
                                // Check if the update was successful
                                if (result.modifiedCount === 1) {
                                    console.log('Document updated successfully');
                                    // Send response back to the client
                                    const responseMessage = JSON.stringify({ code: 1 })
                                    var topicName = content.topic
                                    mqttResponse(topicName, client, responseMessage)
                                } else {
                                    console.log('Document updated successfully');
                                    // Send response back to the client
                                    const responseMessage = JSON.stringify({ code: 1, messages: "FontSize Already updated" })
                                    var topicName = content.topic
                                    mqttResponse(topicName, client, responseMessage)
                                }
                            } catch (error) {
                                // Send response back to the client
                                const responseMessage = JSON.stringify({ code: 0, messages: error })
                                var topicName = content.topic
                                mqttErrorResponse(topicName, client, responseMessage)
                            }


                        } else if (content.topic == '/user/topic/gethistorydates' || content.topic == '/app/topic/gethistorydates') {
                            const collection = db.collection('history');
                            setInterval(
                                async () => {
                                    var final_array = [];

                                    // Query MongoDB for data and send it to the client
                                    const result = collection.find({ "script._id": { $in: content.topicIds } })
                                    await result.forEach((document) => {
                                        if (document) {
                                            final_array.push(document)
                                        }
                                    });
                                    // Send response back to the client
                                    const responseMessage = JSON.stringify(final_array)
                                    var topicName = content.topic
                                    mqttResponse(topicName, client, responseMessage)
                                },
                                300
                            )

                        } else if (content.topic == '/user/topic/gethistory' || content.topic == '/app/topic/gethistory') {
                            const collection = db.collection('history');
                            try {
                                var filter;
                                if (content.p != "") {
                                    content.p = content.p.toUpperCase()
                                    content.s = content.s.toUpperCase()
                                    filter = { "script.s": content.p, "script.e": content.s }

                                } else {
                                    content.s = content.s.toUpperCase()
                                    filter = { "script.e": content.s }
                                }
                                const result = await collection.find(filter);
                                var final_array = [];
                                await result.forEach((document) => {
                                    if (document) {
                                        const date = new Date(document.createdDate);
                                        const dateString = date.toISOString();
                                        var varrrrr = content.d + "T00:00:00.000Z"
                                        if (dateString == varrrrr) {
                                            var data = document.script
                                            data.sn = data._id;
                                            data.s = data.s;
                                            data.ix = data.ix;
                                            data.e = data.e;
                                            data.n = data.n;
                                            data.i1 = data.i1;
                                            data.i2 = data.i2;
                                            data.i3 = data.i3;
                                            data.i4 = data.i4;
                                            data.i6 = data.i6;
                                            data.i9 = data.i9;
                                            data.i10 = data.i10;
                                            data.i11 = data.i11;
                                            data.i12 = data.i12;
                                            data.i19 = data.i19;
                                            data.i20 = data.i20;
                                            data.i5 = data.i5;
                                            data.i7 = data.i7;
                                            data.i8 = data.i8;
                                            data.i13 = data.i13;
                                            data.i14 = data.i14;
                                            data.i15 = data.i15;
                                            data.i16 = data.i16;
                                            data.i17 = data.i17;
                                            data.i18 = data.i18;
                                            data.i21 = data.i21;
                                            data.iltt = data.iltt;
                                            data.si1 = parseInt(data.si1);
                                            data.si2 = parseInt(data.si2);
                                            data.si3 = parseInt(data.si3);
                                            data.si4 = parseInt(data.si4);
                                            data.si5 = parseInt(data.si5);
                                            data.si6 = parseInt(data.si6);
                                            data.si7 = parseInt(data.si7);
                                            data.si8 = parseInt(data.si8);
                                            data.si9 = parseInt(data.si9);
                                            data.si10 = parseInt(data.si10);
                                            data.si11 = parseInt(data.si11);
                                            data.si12 = parseInt(data.si12);
                                            data.si13 = parseInt(data.si13);
                                            data.si14 = parseInt(data.si14);
                                            data.si15 = parseInt(data.si15);
                                            data.si16 = parseInt(data.si16);
                                            data.si17 = parseInt(data.si17);
                                            data.si18 = parseInt(data.si18);
                                            data.si19 = parseInt(data.si19);
                                            data.si20 = parseInt(data.si20);
                                            data.si21 = parseInt(data.si21);
                                            data.stn = parseInt(data.stn);
                                            delete data._id
                                            final_array.push({ a: data })
                                        }
                                    }
                                });
                                const responseMessage = JSON.stringify({ code: 1, data: { d: final_array } })
                                // console.log(responseMessage,'responseMessage')
                                var topicName = content.topic
                                mqttErrorResponse(topicName, client, responseMessage)
                            } catch (error) {
                                const responseMessage = JSON.stringify({ code: 0, messages: error })
                                // console.log(responseMessage,'responseMessage')
                                var topicName = content.topic
                                mqttErrorResponse(topicName, client, responseMessage)
                            }


                        } else if (content.topic == '/user/topic/gethighlow' || content.topic == '/app/topic/gethighlow') {
                            const collection = db.collection('highLowScript');
                            try {
                                var data_array = [];
                                var final_array = [];

                                // Query MongoDB for data and send it to the client
                                const result = collection.find({ scriptId: content.topicIds })
                                await result.forEach((document) => {
                                    if (document) {
                                        data_array.push({
                                            a: document.scriptId,
                                            b: document.high,
                                            c: document.low,
                                            e: document.updateTime
                                        })
                                        final_array.push({
                                            code: 1,
                                            data: { d: data_array }

                                        })
                                    }
                                });
                                // Send response back to the client
                                let responseMessage
                                if (final_array.length > 0) {
                                    responseMessage = JSON.stringify(final_array[0])
                                } else {
                                    responseMessage = JSON.stringify({ code: 0, messages: "No HighLow Data Found!" })
                                }
                                // console.log(responseMessage, 'responseMessage');
                                var topicName = content.topic
                                mqttResponse(topicName, client, responseMessage)
                            } catch (error) {
                                // console.log(responseMessage, 'responseMessage');
                                const responseMessage = JSON.stringify({ code: 0, messages: error })
                                var topicName = content.topic
                                mqttErrorResponse(topicName, client, responseMessage)
                            }

                        } else if (content.topic == '/app/topic/updatecolumns/' || content.topic == '/user/topic/updatecolumns') {
                            const collection = db.collection('userModel');

                            try {
                                // Query MongoDB for data and send it to the client
                                const result = await collection.updateOne(
                                    { "userName": content.d }, // Filter to match the document
                                    {
                                        $set: {
                                            "mobileColumnMap": content.u,
                                        }
                                    }

                                )
                                if (result.modifiedCount === 1) {
                                    // Send response back to the client
                                    const responseMessage = JSON.stringify({ code: 1 })
                                    var topicName = content.topic
                                    mqttResponse(topicName, client, responseMessage)
                                } else {
                                    // Send response back to the client
                                    const responseMessage = JSON.stringify({ code: 1 })
                                    var topicName = content.topic
                                    mqttResponse(topicName, client, responseMessage)
                                }
                            } catch (error) {
                                // Send response back to the client
                                const responseMessage = JSON.stringify({ code: 0, message: error })
                                var topicName = content.topic
                                mqttErrorResponse(topicName, client, responseMessage)
                            }

                        } else if (content.topic == '/user/topic/getclosing' || content.topic == '/app/topic/getclosing') {
                            const collection = db.collection('closingRate');
                            try {
                                var final_array = [];

                                const closingRateData = await collection.find().toArray();
                                var document = closingRateData[0];
                                // console.log(scrdata);
                                // return false
                                if (document) {
                                    var data = document.importDate[content.ScriptTopic]
                                    var data1 = document.data[content.ScriptTopic]
                                    const output = {};

                                    for (const key in data1) {
                                        output[key] = data1[key].map(item => {
                                            return {
                                                b: item.expiryDate,
                                                c: item.close
                                            };
                                        });
                                    }

                                    final_array.push({
                                        code: 1,
                                        data: { bb: data, d: output }
                                    })
                                    // Send response back to the client
                                    const responseMessage = JSON.stringify(final_array[0])
                                    var topicName = content.topic
                                    mqttResponse(topicName, client, responseMessage)
                                } else {

                                    // Send response back to the client
                                    const responseMessage = JSON.stringify({ code: 0, messages: "No Closing Data Found" })
                                    var topicName = content.topic
                                    mqttResponse(topicName, client, responseMessage)
                                }
                            } catch (error) {
                                // Send response back to the client
                                const responseMessage = JSON.stringify({ code: 0, messages: error })
                                var topicName = content.topic
                                mqttErrorResponse(topicName, client, responseMessage)
                            }

                        } else if (content.topic == '/user/topic/updatesettings' || content.topic == '/app/topic/updatesettings/') {
                            const collection = db.collection('userModel');
                            try {
                                var final_array = [];

                                // Query MongoDB for data and send it to the client
                                const result = await collection.updateOne(
                                    { "userName": content.d }, // Filter to match the document
                                    {
                                        $set: {
                                            "showCNXnifty": content.l,
                                            "showIndex": content.k,
                                            "marketColorTheme": content.m,
                                            "contractColorTheme": content.n,
                                            "displayFormat": content.o,
                                            "paginationType": content.z,
                                            "numberofscriptperpage": content.b,
                                        }
                                    }

                                )
                                if (result.modifiedCount === 1) {

                                    // Send response back to the client
                                    const responseMessage = JSON.stringify({ code: 1 })
                                    var topicName = content.topic
                                    mqttResponse(topicName, client, responseMessage)
                                } else {

                                    // Send response back to the client
                                    const responseMessage = JSON.stringify({ code: 1 })
                                    var topicName = content.topic
                                    mqttResponse(topicName, client, responseMessage)
                                }
                            } catch (error) {
                                // Send response back to the client
                                const responseMessage = JSON.stringify({ code: 0, messages: error })
                                var topicName = content.topic
                                mqttErrorResponse(topicName, client, responseMessage)
                            }



                        } else if (content.topic == '/user/topic/changepassword' || content.topic == '/app/topic/changepassword') {
                            const collection = db.collection('userModel');
                            try {
                                // Query MongoDB for data and send it to the client
                                const result = await collection.find({ userName: content.a, password: content.b }).toArray();

                                if (result.length > 0) {
                                    console.log('1')
                                    console.log('content.c', content.c)
                                    const userPasswordChange = await collection.updateOne(
                                        { userName: content.a }, // Filter to match the document
                                        { $set: { "password": content.c } }

                                    )
                                    console.log(userPasswordChange, 'userPasswordChange')
                                    if (userPasswordChange.modifiedCount === 1) {
                                        // Send response back to the client
                                        const responseMessage = JSON.stringify({ code: 1, messages: "Password changed successfully !" })
                                        console.log(responseMessage, 'responseMessage')
                                        var topicName = content.topic
                                        mqttResponse(topicName, client, responseMessage)
                                    } else {
                                        // Send response back to the client
                                        const responseMessage = JSON.stringify({ code: 1, messages: "Password Already changed!" })
                                        console.log(responseMessage, 'responseMessage')
                                        var topicName = content.topic
                                        mqttResponse(topicName, client, responseMessage)
                                    }
                                } else {
                                    console.log('2')
                                    // Send response back to the client
                                    const responseMessage = JSON.stringify({ code: 0, messages: "Something went wrong might be old password does not match" })
                                    console.log(responseMessage, 'responseMessage')
                                    var topicName = content.topic
                                    mqttResponse(topicName, client, responseMessage)
                                }
                            } catch (error) {
                                // Send response back to the client
                                const responseMessage = JSON.stringify({ code: 0, messages: error })
                                console.log(responseMessage, 'responseMessage')
                                var topicName = content.topic
                                mqttErrorResponse(topicName, client, responseMessage)
                            }

                        } else if (content.topic == '/user/topic/signup' || content.topic == '/app/topic/signup') {
                            const collection = db.collection('userModel');
                            try {
                                var final_array = [];
                                var newUsername = content.f.substring(0, 3) + content.h.substring(0, 3)
                                // var newUsername = "9974629466"
                                // Query MongoDB for data and send it to the client
                                const insertUser = {
                                    // topic: topic,h:"7016581406",w:["NSE","BSE","MCX"],f:"Hemil",g:"Shah",p:"Ahmedabad"

                                    "numberofscriptperpage": 100,
                                    "fontSize": 5,
                                    "userName": newUsername,
                                    "password": "123456",
                                    "firstName": content.f,
                                    "lastName": content.g,
                                    "contactNo": content.h,
                                    "status": true,
                                    "showIndex": true,
                                    "showCNXnifty": true,
                                    "marketColorTheme": "BLACK",
                                    "contractColorTheme": "WHITE",
                                    "displayFormat": "Custom",
                                    "city": content.p,
                                    "macId": "",
                                    "subscriptionType": "",
                                    "expiryDate": "",
                                    "subscriptionDate": "",
                                    "mobileColumnMap": {},
                                    "userScriptsByProfileMap": [],
                                    "assignedSegment": content.w,
                                    "paginationType": true,
                                    "tradeAllowed": false,
                                    "lastLogin": "",
                                    "allowedDevices": {},
                                    "deviceType": "",
                                    "_class": "com.live.market.model.UserModel"
                                }
                                const findUser = await collection.find({
                                    $or: [
                                        { userName: newUsername },
                                        { contactNo: content.h }
                                    ]
                                }).toArray();
                                //   console.log(findUser,'findUser')
                                //   return false
                                if (findUser.length > 0) {
                                    // Send response back to the client
                                    const responseMessage = JSON.stringify({ code: 0, messages: "User already registered on this number,for more information contect 9924440145" })
                                    var topicName = content.topic
                                    mqttResponse(topicName, client, responseMessage)
                                } else {

                                    const result = await collection.insertOne(insertUser);

                                    // Retrieve the ID of the last inserted document
                                    const lastInsertedId = result.insertedId;
                                    if (lastInsertedId) {
                                        // Send response back to the client
                                        const responseMessage = JSON.stringify({ code: 1, messages: `User signup successfully. Your UserName is ${newUsername} and you password is 123456` })
                                        var topicName = content.topic
                                        mqttResponse(topicName, client, responseMessage)
                                    } else {
                                        // Send response back to the client
                                        const responseMessage = JSON.stringify({ code: 0, messages: `User Not signup successfully` })
                                        var topicName = content.topic
                                        mqttResponse(topicName, client, responseMessage)
                                    }
                                }
                            } catch (error) {
                                // Send response back to the client
                                const responseMessage = JSON.stringify({ code: 0, messages: error })
                                var topicName = content.topic
                                mqttErrorResponse(topicName, client, responseMessage)
                            }

                        } else if (content.topic == '/user/topic/forgotpassword' || content.topic == '/app/topic/forgotpassword') {
                            const collection = db.collection('userModel');

                            try {
                                var final_array = [];

                                // Query MongoDB for data and send it to the client
                                const result = await collection.find({ userName: content.userName }).toArray();

                                if (result.length > 0) {
                                    var newPassword = "123456"
                                    const userData = await collection.updateOne({ userName: content.userName }, { $set: { password: newPassword } });
                                    if (userData.modifiedCount === 1) {
                                        // Send response back to the client
                                        const responseMessage = JSON.stringify({ code: 1, messages: `We set you password as ${newPassword} for this username ${content.userName}` })
                                        var topicName = content.topic
                                        mqttResponse(topicName, client, responseMessage)

                                    } else {
                                        // Send response back to the client
                                        const responseMessage = JSON.stringify({ code: 1, messages: `Password already set to the ${newPassword}` })
                                        var topicName = content.topic
                                        mqttResponse(topicName, client, responseMessage)
                                    }
                                } else {
                                    // Send response back to the client
                                    const responseMessage = JSON.stringify({ code: 0, messages: `User Not found for this username ${content.userName}` })
                                    var topicName = content.topic
                                    mqttResponse(topicName, client, responseMessage)
                                }
                            } catch (error) {
                                // Send response back to the client
                                const responseMessage = JSON.stringify({ code: 0, messages: error })
                                var topicName = content.topic
                                mqttErrorResponse(topicName, client, responseMessage)
                            }

                        }
                    } else {
                    }
                })
            })
            //     }
            // }

        } catch (error) {
            console.error('Invalid JSON:', error.message);
        }
    }
});
async function pollForUpdates(collection, lastCheckDate, content) {
    const currentDate = new Date();
    // const updatedDocumxczczxczxczxcents = await collection.aggregate([
    //     {
    //         $match: {
    //             updatedAt: { $gt: lastCheckDate },
    //             id: { $in: content.a }
    //         }
    //     },
    //     {
    //         $lookup: {
    //             from: collection.collectionName,
    //             localField: "id",
    //             foreignField: "id",
    //             as: "previousDocument"
    //         }
    //     },
    //     {
    //         $unwind: {
    //             path: "$previousDocument",
    //             preserveNullAndEmptyArrays: true
    //         }
    //     },
    //     {
    //         $addFields: {
    //             updatedFields: {
    //                 $objectToArray: "$$ROOT"
    //             },
    //             previousFields: {
    //                 $objectToArray: { $ifNull: ["$previousDocument", {}] }
    //             }
    //         }
    //     },
    //     {
    //         $project: {
    //             id: 1,
    //             updatedAt: 1,
    //             updatedFields: {
    //                 $filter: {
    //                     input: "$updatedFields",
    //                     as: "field",
    //                     cond: {
    //                         $let: {
    //                             vars: {
    //                                 previousValue: {
    //                                     $arrayElemAt: [
    //                                         {
    //                                             $filter: {
    //                                                 input: "$previousFields",
    //                                                 cond: { $eq: ["$$this.k", "$$field.k"] }
    //                                             }
    //                                         },
    //                                         0
    //                                     ]
    //                                 }
    //                             },
    //                             in: {
    //                                 $ne: ["$$field.v", "$$previousValue.v"]
    //                             }
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //     },
    // ]).toArray();
    // console.log(updatedDocumxczczxczxczxcents,'updatedDocumxczczxczxczxcents');
    //this will not give accurate result
    // const updatedDocuments = await collection.aggregate([
    //     {
    //         $match: {
    //             updatedAt: { $gt: lastCheckDate },
    //             id: { $in: content.a }
    //         }
    //     },
    //     {
    //         $lookup: {
    //             from: collection.collectionName,
    //             localField: "id",
    //             foreignField: "id",
    //             as: "previousDocument"
    //         }
    //     },
    //     {
    //         $unwind: {
    //             path: "$previousDocument",
    //             preserveNullAndEmptyArrays: true
    //         }
    //     },
    //     {
    //         $addFields: {
    //             updatedFields: {
    //                 $objectToArray: "$$ROOT"
    //             },
    //             previousFields: {
    //                 $objectToArray: { $ifNull: ["$previousDocument", {}] }
    //             }
    //         }
    //     },
    //     {
    //         $project: {
    //             id: 1,
    //             updatedAt: 1,
    //             updatedFields: {
    //                 $filter: {
    //                     input: "$updatedFields",
    //                     as: "field",
    //                     cond: {
    //                         $let: {
    //                             vars: {
    //                                 previousValue: {
    //                                     $ifNull: [
    //                                         {
    //                                             $arrayElemAt: [
    //                                                 {
    //                                                     $filter: {
    //                                                         input: "$previousFields",
    //                                                         as: "prevField",
    //                                                         cond: { $eq: ["$$prevField.k", "$$field.k"] }
    //                                                     }
    //                                                 },
    //                                                 0
    //                                             ]
    //                                         },
    //                                         { v: null }
    //                                     ]
    //                                 }
    //                             },
    //                             in: {
    //                                 $ne: ["$$field.v", "$$previousValue.v"]
    //                             }
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //     },
    //     {
    //         $project: {
    //             id: 1,
    //             updatedAt: 1,
    //             updatedFields: {
    //                 $arrayToObject: "$updatedFields"
    //             }
    //         }
    //     }
    // ]).toArray();
    //////////////////////////////////////////////////////////////////////////////
    const updatedDocuments = await collection.aggregate([
        {
            $match: {
                updatedAt: { $gt: lastCheckDate },
                id: { $in: content.a }
            }
        },
        {
            $lookup: {
                from: collection.collectionName,
                localField: "id",
                foreignField: "id",
                as: "previousDocument"
            }
        },
        {
            $unwind: {
                path: "$previousDocument",
                preserveNullAndEmptyArrays: true
            }
        },
        {
            $addFields: {
                updatedFields: {
                    $filter: {
                        input: {
                            $map: {
                                input: { $objectToArray: "$$ROOT" },
                                as: "currentField",
                                in: {
                                    k: "$$currentField.k",
                                    v: "$$currentField.v",
                                    prevV: {
                                        $arrayElemAt: [
                                            {
                                                $map: {
                                                    input: { $objectToArray: "$previousDocument" },
                                                    as: "prevField",
                                                    in: {
                                                        $cond: {
                                                            if: { $eq: ["$$prevField.k", "$$currentField.k"] },
                                                            then: "$$prevField.v",
                                                            else: "$$REMOVE"
                                                        }
                                                    }
                                                }
                                            },
                                            0
                                        ]
                                    }
                                }
                            }
                        },
                        as: "field",
                        cond: { $ne: ["$$field.v", "$$field.prevV"] }
                    }
                }
            }
        },
        {
            $project: {
                id: 1,
                updatedAt: 1,
                updatedFields: {
                    $arrayToObject: {
                        $map: {
                            input: "$updatedFields",
                            as: "field",
                            in: { k: "$$field.k", v: "$$field.v" }
                        }
                    }
                }
            }
        }
    ]).toArray();



    //////3rd working query with low accuracy/////////////////////////    
    // const updatedDocuments = await collection.aggregate([
    //     {
    //         $match: {
    //             updatedAt: { $gt: lastCheckDate },
    //             id: { $in: content.a }
    //         }
    //     },
    //     {
    //         $lookup: {
    //             from: collection.collectionName,
    //             localField: "id",
    //             foreignField: "id",
    //             as: "previousDocument"
    //         }
    //     },
    //     {
    //         $unwind: {
    //             path: "$previousDocument",
    //             preserveNullAndEmptyArrays: true
    //         }
    //     },
    //     {
    //         $addFields: {
    //             updatedFields: {
    //                 $objectToArray: "$$ROOT"
    //             },
    //             previousFields: {
    //                 $objectToArray: { $ifNull: ["$previousDocument", {}] }
    //             }
    //         }
    //     },
    //     {
    //         $addFields: {
    //             changedFields: {
    //                 $filter: {
    //                     input: {
    //                         $map: {
    //                             input: "$updatedFields",
    //                             as: "field",
    //                             in: {
    //                                 k: "$$field.k",
    //                                 v: {
    //                                     updatedValue: "$$field.v",
    //                                     previousValue: {
    //                                         $arrayElemAt: [
    //                                             {
    //                                                 $map: {
    //                                                     input: {
    //                                                         $filter: {
    //                                                             input: "$previousFields",
    //                                                             as: "prevField",
    //                                                             cond: { $eq: ["$$prevField.k", "$$field.k"] }
    //                                                         }
    //                                                     },
    //                                                     as: "prevField",
    //                                                     in: "$$prevField.v"
    //                                                 }
    //                                             },
    //                                             0
    //                                         ]
    //                                     }
    //                                 }
    //                             }
    //                         }
    //                     },
    //                     as: "field",
    //                     cond: { $ne: ["$$field.v.updatedValue", "$$field.v.previousValue"] }
    //                 }
    //             }
    //         }
    //     },
    //     {
    //         $project: {
    //             id: 1,
    //             updatedAt: 1,
    //             updatedFields: {
    //                 $arrayToObject: {
    //                     $map: {
    //                         input: "$changedFields",
    //                         as: "field",
    //                         in: {
    //                             k: "$$field.k",
    //                             v: "$$field.v.updatedValue"
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //     }
    // ]).toArray();
    //////3rd working query with low accuracy/////////////////////////    

    // console.log(updatedDocuments, 'updatedDocuments')
    // console.log(updatedDocuments, 'updatedDocuments')
    return { currentDate, updatedDocuments };
}
function mqttResponse(topicName, client, responseMessage) {
    mqttServer.publish({
        topic: topicName + '/' + client.id,
        payload: responseMessage,
    }, function (err) {
        if (err) {
            console.error('Error sending response to client', client.id, ':', err);
        } else {
            console.log('Response sent to client for this topic', topicName + '/' + client.id);
        }
    });
}
function mqttErrorResponse(topicName, client, responseMessage) {

    mqttServer.publish({
        topic: topicName + '/' + client.id, // Assuming client.id is unique
        payload: responseMessage,
    }, function (err) {
        if (err) {
            console.error('Error sending response to client', client.id, ':', err);
        } else {
            console.log('Response sent to client', client.id);
            // clearInterval(interval); // Clear the interval after sending the response
        }
    });
}
function processUpdatedDocument(document) {
    if (!document) return null;

    delete document.updatedFields?.previousDocument;
    if (Object.keys(document.updatedFields).length === 0) return null;

    const processedDoc = { ...document, ...document.updatedFields };
    delete processedDoc.updatedFields;
    delete processedDoc._id;
    delete processedDoc.__v;
    delete processedDoc.createdAt;
    delete processedDoc.updatedAt;
    processedDoc.sn = processedDoc.id;
    delete processedDoc.id;

    for (const key in processedDoc) {
        if (processedDoc[key] === undefined) {
            delete processedDoc[key];
        }
    }
    return processedDoc;
}

function processInitialDocument(document) {
    if (!document) return null;

    delete document._id;
    delete document.__v;
    delete document.createdAt;
    delete document.updatedAt;

    document.sn = document.id;
    delete document.id;

    const siFields = ['si1', 'si2', 'si3', 'si4', 'si5', 'si6', 'si7', 'si8', 'si9', 'si10', 'si11', 'si12', 'si13', 'si14', 'si15', 'si16', 'si17', 'si18', 'si19', 'si20', 'si21'];
    siFields.forEach(field => {
        if (document[field] !== undefined) {
            document[field] = parseInt(document[field]);
        }
    });

    if (document.stn !== undefined) {
        document.stn = parseInt(document.stn);
    }

    return document;
}

function convertSiFieldsToInt(obj) {
    for (const key in obj) {
        if (key.includes('si')) {
            obj[key] = parseInt(obj[key]);
        }
    }
    if (obj.stn !== undefined) {
        obj.stn = parseInt(obj.stn);
    }
}
function processIndexDocument(document) {
    document.i6 = document.i6;
    document.si6 = parseInt(document.si6);
    document.i19 = document.i19;
    document.si19 = parseInt(document.si19);
    delete document._id;
    delete document.__v;
    delete document.createdAt;
    delete document.updatedAt;
    delete document.s;
    delete document.ix;
    delete document.e;
    delete document.n;
    delete document.i1;
    delete document.i2;
    delete document.i3;
    delete document.i4;
    delete document.i9;
    delete document.i10;
    delete document.i11;
    delete document.i12;
    delete document.i20;
    delete document.i5;
    delete document.i7;
    delete document.i8;
    delete document.i13;
    delete document.i14;
    delete document.i15;
    delete document.i16;
    delete document.i17;
    delete document.i18;
    delete document.i21;
    delete document.iltt;
    delete document.si1;
    delete document.si2;
    delete document.si3;
    delete document.si4;
    delete document.si5;
    delete document.si7;
    delete document.si8;
    delete document.si9;
    delete document.si10;
    delete document.si11;
    delete document.si12;
    delete document.si13;
    delete document.si14;
    delete document.si15;
    delete document.si16;
    delete document.si17;
    delete document.si18;
    delete document.si20;
    delete document.si21;
    delete document.stn;
    delete document.id;
}
module.exports = {
    mqttServer,
    clientSubscriptions,
    clientIntervals,
    DB_AUTH_URL
};
