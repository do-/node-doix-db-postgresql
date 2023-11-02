const {DbLangPg} = require ('..'), lang = new DbLangPg ()

test ('normalizeSQL', () => {

	const normalizeSQL = sql => {

		const call = {sql}

		lang.normalizeSQL (call)

		return call.sql

	}

	expect (normalizeSQL ('SELECT 1-1/2--')).toBe ('SELECT 1-1/2')

	expect (normalizeSQL ('SELECT * FROM t WHERE id = ?')).toBe ('SELECT * FROM t WHERE id = $1')
	
	expect (normalizeSQL ('SELECT * FROM t WHERE id = ? AND label LIKE ?')).toBe ('SELECT * FROM t WHERE id = $1 AND label LIKE $2')

	expect (normalizeSQL ('SELECT * FROM t WHERE id::jsonb ? ? AND label LIKE ?')).toBe ('SELECT * FROM t WHERE id::jsonb ? $1 AND label LIKE $2')

	expect (normalizeSQL ("SELECT * FROM t WHERE id = ? AND label='Don''t you know?'")).toBe ("SELECT * FROM t WHERE id = $1 AND label='Don''t you know?'")

	expect (normalizeSQL ('SELECT * FROM t /*What /*the he// is*/t?*/ WHERE id = ?')).toBe ('SELECT * FROM t  WHERE id = $1')

	expect (normalizeSQL (`
		SELECT
			id
--			, label ???
		FROM
			t
		WHERE
			id = ?
	`).trim ().replace (/\s+/g, ' ')).toBe (`SELECT id FROM t WHERE id = $1`)

})