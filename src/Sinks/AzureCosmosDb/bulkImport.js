﻿function bulkImport(docs) {
	var collection = getContext().getCollection();

	// The count of imported docs, also used as current doc index.
	var count = 0;
    // Validate input.
	if (!docs) throw new Error("The array is undefined or null.");

	var docsLength = docs.length;
	if (docsLength == 0 || docsLength == undefined) {
		getContext().getResponse().setBody(0);
	}
	else {

		for (var i = 0; i < docsLength; i++) {
			var accepted = collection.createDocument(collection.getSelfLink(),
				docs[i],
				function (err, documentCreated) {
					if (err) {
                        console.log(err);
                    }
				});
			if (!accepted) break;
			count++;
		}
	}

	getContext().getResponse().setBody(count);
}
