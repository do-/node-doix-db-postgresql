module.exports = {

    label: 'Procedure 1',
    
    parameters: [
    	'id int',
    ],

    lang: 'plpgsql',

    body: `
    	BEGIN
    		INSERT INTO tb_1 (id) VALUES (f_1 (id));
    	END;
    `,

}