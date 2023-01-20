const {DbLangPg} = require ('..'), lang = new DbLangPg ()

test ('normalizeSQL', () => {

	expect (lang.normalizeSQL ('SELECT 1-1/2--')).toBe ('SELECT 1-1/2')

	expect (lang.normalizeSQL ('SELECT * FROM t WHERE id = ?')).toBe ('SELECT * FROM t WHERE id = $1')
	
	expect (lang.normalizeSQL ('SELECT * FROM t WHERE id = ? AND label LIKE ?')).toBe ('SELECT * FROM t WHERE id = $1 AND label LIKE $2')

	expect (lang.normalizeSQL ('SELECT * FROM t WHERE id::jsonb ? ? AND label LIKE ?')).toBe ('SELECT * FROM t WHERE id::jsonb ? $1 AND label LIKE $2')

	expect (lang.normalizeSQL ("SELECT * FROM t WHERE id = ? AND label='Don''t you know?'")).toBe ("SELECT * FROM t WHERE id = $1 AND label='Don''t you know?'")

	expect (lang.normalizeSQL ('SELECT * FROM t /*What /*the he// is*/t?*/ WHERE id = ?')).toBe ('SELECT * FROM t  WHERE id = $1')

	expect (lang.normalizeSQL (`
		SELECT
			id
--			, label ???
		FROM
			t
		WHERE
			id = ?
	`).trim ().replace (/\s+/g, ' ')).toBe (`SELECT id FROM t WHERE id = $1`)

})