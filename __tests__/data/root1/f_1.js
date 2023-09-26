module.exports = {

    comment: 'Function 1',
    
    parameters: [
    	'id int',
    	'label text = NULL',
    ],

    returns: 'int',

    lang: 'sql',

    body: 'SELECT id + 1',

}