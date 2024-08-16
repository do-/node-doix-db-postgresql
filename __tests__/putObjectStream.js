const Path = require ('path')
const {DbModel} = require ('doix-db')
const MockJob = require ('./lib/MockJob.js'), job = new MockJob ()
const {DbPoolPg} = require ('..')
const {Readable} = require ('stream')

const pool = new DbPoolPg ({
	db: {
		connectionString: process.env.CONNECTION_STRING,
	},
})

pool.logger = job.logger

const src = Path.join (__dirname, 'data', 'root1')

afterAll(async () => {

	await pool.pool.end ()

})

test ('bad args', async () => {

	const m = new DbModel ({src, db: pool})

	m.loadModules ()

	try {

		var db = await pool.setResource (job, 'db')

//		await expect (db.putObjectStream ('tb_2', ['id', 'label'], {objectMode: 1})).rejects.toThrow ()
		await expect (db.putObjectStream ('tb_...', ['id', 'label'], {})).rejects.toThrow ()

	}	
	finally {

		await db.release ()

	}


})

test ('basic', async () => {

	const m = new DbModel ({src, db: pool})

	m.loadModules ()

	try {

		var db = await pool.setResource (job, 'db')
		
		await db.do ('DROP TABLE IF EXISTS tb_2')
		await db.createTempTable ('tb_2')

		{

			const src = [
				{id: 1, label: 'admin'},
				{id: 2, label: 'user'},
			]

			await db.insert ('tb_2', src)

			expect (await db.getArray ('SELECT * FROM tb_2 ORDER BY id')).toStrictEqual (src)

		}

	}
	
	finally {

		await db.release ()

	}

})

test ('bad data, xform', async () => {

	const m = new DbModel ({src, db: pool})

	m.loadModules ()

	let err

	try {

		var db = await pool.setResource (job, 'db')
		
		await db.do ('DROP TABLE IF EXISTS tb_2')
		await db.createTempTable ('tb_2')

		{

			const src = [
				{id: Infinity, label: 'admin'},
				{id: 2, label: 'user'},
			]
		
			const os = await db.putObjectStream ('tb_2', ['id', 'label'])

			await new Promise ((ok, fail) => {
				os.on ('error', fail)
				os.on ('complete', ok)
				Readable.from (src).pipe (os)
			})

		}

	}	
	catch (x) {

		err = x

	}
	finally {

		await db.release ()

	}

	expect (err.message).toMatch (/support/)

})

test ('bad data, db', async () => {

	const m = new DbModel ({src, db: pool})

	m.loadModules ()

	let err

	try {

		var db = await pool.setResource (job, 'db')
		
		await db.do ('DROP TABLE IF EXISTS tb_2')
		await db.createTempTable ('tb_2')
		await db.do ('ALTER TABLE tb_2 ADD PRIMARY KEY (id)')

		{

			const src = [
				{id: 1, label: 'admin'},
				{id: 1, label: 'user'},
			]
		
			const os = await db.putObjectStream ('tb_2', ['id', 'label'], {objectMode: true})

			await new Promise ((ok, fail) => {
				os.on ('error', fail)
				os.on ('complete', ok)
				Readable.from (src).pipe (os)
			})

		}

	}
	catch (x) {

		err = x

	}	
	finally {

		await db.release ()

	}

	expect (err.message).toMatch (/pk/)

})
