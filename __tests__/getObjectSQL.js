const Path = require ('path')
const MockJob = require ('./lib/MockJob.js'), job = new MockJob ()
const {DbPoolPg} = require ('..')

const pool = new DbPoolPg ({
	db: {
		connectionString: process.env.CONNECTION_STRING,
	},
})

pool.logger = job.logger

const r = () => ['root1'].map (i => Path.join (__dirname, 'data', i))

const dir = {
	root: r (),
	live: false,
}

afterAll(async () => {

	await pool.pool.end ()

})

test ('no model', async () => {

	try {

		var db = await pool.setResource (job, 'db')

		const o = await db.getObject ('SELECT ?', [1])
	
	}
	finally {

		await db.release ()

	}

})

