const catalyst = require('zcatalyst-sdk-node');

module.exports = async (context, basicIO) => {
	try {
		const catalystApp = catalyst.initialize(context);

		const offset = parseInt(basicIO.getArgument('offset'));

		const totalRecords = 200;

		const aTable = catalystApp.datastore().table('A');
		const bTable = catalystApp.datastore().table('B');
		const cTable = catalystApp.datastore().table('C');
		const dTable = catalystApp.datastore().table('D');

		const aRecords = await aTable.insertRows(
			Array.from({ length: totalRecords }, (_, i) => ({
				VALUE: offset + i
			}))
		);
		await bTable.insertRows(
			Array.from({ length: totalRecords }, (_, i) => ({
				VALUE: offset + i,
				AID: aRecords[i].ROWID
			}))
		);

		const cRecords = await cTable.insertRows(
			Array.from({ length: totalRecords }, (_, i) => ({
				VALUE: offset + i
			}))
		);

		await cTable.updateRows(
			cRecords.map(({ ROWID }) => ({
				ROWID,
				CID: ROWID
			}))
		);

		await dTable.insertRows(
			Array.from({ length: totalRecords }, (_, i) => ({
				VALUE: offset + i
			}))
		);

		basicIO.write('success');
	} catch (err) {
		basicIO.write(err);
	}

	context.close();
};
