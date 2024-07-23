const { MongoClient } = require("mongodb");

// MongoDB connection details
const oldDbUri = "mongodb://localhost:27017/gwappdb";
const newDbUri = "mongodb://localhost:27017/newgwappdb";

async function findAndCopyMissingFields() {
    console.time('Execution Time');

    const oldClient = new MongoClient(oldDbUri);
    const newClient = new MongoClient(newDbUri);

    try {
        await oldClient.connect();
        await newClient.connect();

        const oldDb = oldClient.db("gwappdb");
        const newDb = newClient.db("newgwappdb");

        const oldCollection = oldDb.collection("requisitions");
        const newCollection = newDb.collection("requisitions");

        const totalOldDocuments = await oldCollection.countDocuments({});
        const totalNewDocuments = await newCollection.countDocuments({});

        console.log(`Total documents in old collection: ${totalOldDocuments}`);
        console.log(`Total documents in new collection: ${totalNewDocuments}`);

        if (totalOldDocuments === 0) {
            console.log("Old collection has no documents.");
            return;
        }
        if (totalNewDocuments === 0) {
            console.log("New collection has no documents.");
            return;
        }

        // Query all documents from old database
        const oldDocuments = await oldCollection.find().toArray();

        let missingFieldsCount = 0;
        let updatedDocumentsCount = 0;

        // Iterate through each document and check if it exists in new database
        for (const oldDoc of oldDocuments) {
            const { title, total, name, date } = oldDoc;

            // Query by title, total, name, and date in new database
            const matchingDoc = await newCollection.findOne({
                title,
                total,
                name,
                date,
            });

            // If a matching document is found, check for missing fields
            if (matchingDoc) {
                const missingFields = Object.keys(oldDoc).filter(
                    (field) => !(field in matchingDoc)
                );

                if (missingFields.length > 0) {
                    const updateFields = missingFields.reduce((acc, field) => {
                        acc[field] = oldDoc[field];
                        return acc;
                    }, {});
                    
                    // Update the new document with the missing fields from the old document
                    await newCollection.updateOne(
                        { _id: matchingDoc._id },
                        { $set: updateFields }
                    );
                    missingFieldsCount++;
                    updatedDocumentsCount++;
                }
            }
        }

        console.log(
            `Total number of documents with missing fields: ${missingFieldsCount} out of ${totalOldDocuments}`
        );
        console.log(
            `Total number of updated documents in new database: ${updatedDocumentsCount}`
        );
    } catch (err) {
        console.error(err);
    } finally {
        await oldClient.close();
        await newClient.close();
        console.timeEnd('Execution Time');
    }
}

findAndCopyMissingFields();
