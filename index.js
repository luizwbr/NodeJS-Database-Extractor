(async() => {
    const fs = require("fs");
    const { Transform } = require('stream');
    const parquet = require('parquetjs');
    const readline = require('readline');

    const knex = require('knex')({
        client: 'pg',
        version: '10.5',
        connection: {
            host : 'localhost',
            user : 'postgre',
            password : 'post',
            database : 'config',
            port : '5433'
        }
    });

    const toFile = fs.createWriteStream('./config.file');

    const toJson = new Transform({
        objectMode: true,
        transform(chunk, encoding, callback) {
            this.push(JSON.stringify(chunk))
            callback()
        }
    });

    await knex
        .select('*')
        .from('config')
        .stream((stream) => {
            stream
                .pipe(toJson)
                .pipe(toFile);
        })
        .then(() => {
            toFile.end();
        })
        .catch((e) => { console.error(e); });

    const schema = new parquet.ParquetSchema({
        id: { type: 'INT64' },
        schema_name: { type: 'UTF8' },
        table_name: { type: 'UTF8' },
        environment: { type: 'UTF8' },
        primary_keys: { type: 'UTF8' },
        full_load: { type: 'BOOLEAN' },
        jndi_connection_id: { type: 'BOOLEAN' },
        active: { type: 'BOOLEAN' }
    });
   
    let rl = readline.createInterface({
        input: fs.createReadStream('./config.file')
    });
    
    let line_no = 0;
    
    const writer = await parquet.ParquetWriter.openFile(schema, './config.parquet');

    rl.on('line', function(line) {
        line_no++;
        const contentLine = JSON.parse(line);
        writer.appendRow(contentLine);
    });
    
    rl.on('close', function(line) {
        writer.close();
        console.log('Total lines : ' + line_no);
        process.exit();
    });
})();