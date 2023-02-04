const Path = require ('path')
const {DbModel} = require ('doix-db')
const MockJob = require ('./lib/MockJob.js'), job = new MockJob ()
const {DbClientPg, DbPoolPg} = require ('..')

const pool = new DbPoolPg ({
	db: {
		connectionString: process.env.CONNECTION_STRING,
	},
})

const r = () => ['root1'].map (i => Path.join (__dirname, 'data', i))

const dir = {
	root: r (),
	live: false,
}

afterAll(async () => {

	await pool.pool.end ()

})

test ('basic', async () => {

	try {

		var db = await pool.toSet (job, 'db')

		const m = new DbModel ({dir, db: pool})

		m.loadModules ()

		const q = m.createQuery ([
			['tb_1', {
				filters: [
					['id', '=', 1],
					['label', '~', '.'],
					['label', '~*', '.'],
					['label', '!~', '.'],
					['label', '!~*', '.'],
					['label', 'ILIKE', '%'],
					['label', 'NOT ILIKE', '%'],
					['label', 'SIMILAR TO', '%'],
					['label', 'NOT SIMILAR TO', '%'],
				]
			}],
		])

		expect (q.toParamsSql ()).toStrictEqual ([1, '.', '.', '.', '.', '%', '%', '%', '%', 'SELECT "tb_1"."id" AS "id","tb_1"."label" AS "label" FROM "tb_1" AS "tb_1" WHERE "tb_1"."id" = ? AND "tb_1"."label" ~ ? AND "tb_1"."label" ~* ? AND "tb_1"."label" !~ ? AND "tb_1"."label" !~* ? AND "tb_1"."label" ILIKE ? AND "tb_1"."label" NOT ILIKE ? AND "tb_1"."label" SIMILAR TO ? AND "tb_1"."label" NOT SIMILAR TO ?'])

	}
	finally {

		await db.release ()

	}

})