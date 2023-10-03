const Path = require ('path')
const {DbModel} = require ('doix-db')
const MockJob = require ('./lib/MockJob.js'), job = new MockJob ()
const {DbPoolPg} = require ('..')

const pool = new DbPoolPg ({
	db: {
		connectionString: process.env.CONNECTION_STRING,
	},
})

const src = Path.join (__dirname, 'data', 'root1')

afterAll(async () => {

	await pool.pool.end ()

})

test ('op', async () => {

	try {

		var db = await pool.toSet (job, 'db')

		const m = new DbModel ({src, db: pool})

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
		], {order: ['label']})

		expect (q.toParamsSql ()).toStrictEqual ([1, '.', '.', '.', '.', '%', '%', '%', '%', 'SELECT "tb_1"."id" AS "id","tb_1"."label" AS "label","tb_1"."amount" AS "amount","tb_1"."cnt" AS "cnt" FROM "tb_1" AS "tb_1" WHERE "tb_1"."id" = ? AND "tb_1"."label" ~ ? AND "tb_1"."label" ~* ? AND "tb_1"."label" !~ ? AND "tb_1"."label" !~* ? AND "tb_1"."label" ILIKE ? AND "tb_1"."label" NOT ILIKE ? AND "tb_1"."label" SIMILAR TO ? AND "tb_1"."label" NOT SIMILAR TO ? ORDER BY "tb_1"."label"'])

	}
	finally {

		await db.release ()

	}

})

test ('limit', async () => {

	try {

		var db = await pool.toSet (job, 'db')

		const m = new DbModel ({src, db: pool})

		m.loadModules ()

		const q = m.createQuery ([
			['tb_1'],
		], {order: ['label'], limit: 5, offset: 10})

		expect (q.toParamsSql ()).toStrictEqual ([5, 10, 'SELECT "tb_1"."id" AS "id","tb_1"."label" AS "label","tb_1"."amount" AS "amount","tb_1"."cnt" AS "cnt" FROM "tb_1" AS "tb_1" ORDER BY "tb_1"."label" LIMIT ? OFFSET ?'])

	}
	finally {

		await db.release ()

	}

})