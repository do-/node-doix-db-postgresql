module.exports = {

    comment: 'Procedure 1',
    
    parameters: [
    	'id int=0',
    ],

    lang: 'plpgsql',

    body: `
    	BEGIN
    		INSERT INTO tb_1 (id) VALUES (f_1 (id));
    	END;
    `,

}