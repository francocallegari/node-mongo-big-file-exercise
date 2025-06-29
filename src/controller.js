const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const Records = require('./records.model');

const BATCH_SIZE = 1000;

const upload = async (req, res) => {
    const { file } = req;

    if (!file) {
        return res.status(400).json({ message: 'Archivo no proporcionado.' });
    }

    const filePath = path.resolve(file.path);
    const recordsBatch = [];
    let totalInserted = 0;
    let responseSent = false;

    const sendResponse = (status, body) => {
        if (!responseSent) {
            responseSent = true;
            res.status(status).json(body);
        }
    };

    const stream = fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', async (row) => {
            try {
                if (row.id && row.firstname && row.lastname && row.email) {
                    recordsBatch.push({
                        id: Number(row.id),
                        firstname: row.firstname,
                        lastname: row.lastname,
                        email: row.email,
                        email2: row.email2,
                        profession: row.profession,
                    });
                }

                if (recordsBatch.length >= BATCH_SIZE) {
                    stream.pause();
                    await Records.insertMany(recordsBatch);
                    totalInserted += recordsBatch.length;
                    recordsBatch.length = 0;
                    stream.resume();
                }
            } catch (err) {
                stream.destroy(err);
            }
        })
        .on('end', async () => {
            try {
                if (recordsBatch.length > 0) {
                    await Records.insertMany(recordsBatch);
                    totalInserted += recordsBatch.length;
                }

                fs.unlink(filePath, (err) => {
                    if (err) console.error('Error al eliminar el archivo temporal:', err);
                });

                sendResponse(200, {
                    message: `Archivo procesado correctamente. Total registros insertados: ${totalInserted}`,
                });
            } catch (err) {
                fs.unlink(filePath, () => {});
                sendResponse(500, { message: 'Error al insertar registros finales', error: err.message });
            }
        })
        .on('error', (err) => {
            fs.unlink(filePath, () => {});
            sendResponse(500, { message: 'Error al procesar el archivo', error: err.message });
        });
};

const list = async (_, res) => {
    try {
        const data = await Records.find({}).sort({ _id: -1 }).limit(10).lean();
        return res.status(200).json(data);
    } catch (err) {
        return res.status(500).json(err);
    }
};

module.exports = {
    upload,
    list,
};