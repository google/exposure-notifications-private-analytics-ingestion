const firebase = require('@firebase/testing');
const fs = require('fs');
const path = require('path');

const projectId = "emulator-test-project"

beforeAll(async () => {
  await firebase.loadFirestoreRules({
    projectId: projectId,
    rules: fs.readFileSync("firestore.rules", "utf8")
  });
});

beforeEach(async () => {
  await firebase.clearFirestoreData({projectId});
});

afterAll(async () => {
  await Promise.all(firebase.apps().map((app) => app.delete()));
});

describe('Tests of document creation', () => {
  const app = firebase.initializeTestApp({
    projectId: projectId,
    auth: null
  });
  const db = app.firestore()
  it('document cannot be created without uuid',
      async () => {
        const doc = db.collection('uuid').doc('foo');
        await firebase.assertFails(doc.set({}));
      });
  it('document can be created if path matches uuid',
      async () => {
        const doc = db.collection('uuid').doc('foo')
                      .collection('date').doc('2020-09-03-14')
                      .collection('metrics').doc('testMetric');
        await firebase.assertSucceeds(doc.set({'uuid': 'foo'}));
      });
  it('document cannot be deleted',
      async () => {
        const doc = db.collection('uuid').doc('foo')
                      .collection('date').doc('2020-09-03-14')
                      .collection('metrics').doc('testMetric');
        await firebase.assertFails(doc.delete());
      });
  it('document cannot be updated',
      async () => {
        const doc = db.collection('uuid').doc('foo')
                      .collection('date').doc('2020-09-03-14')
                      .collection('metrics').doc('testMetric');
        await firebase.assertFails(doc.update({'uuid': 'foo'}));
      });
// TODO: workaround jest issue and uncomment: https://github.com/firebase/firebase-js-sdk/issues/3096
//  it('document cannot be read from server',
//      async () => {
//        const doc = db.collection('uuid').doc('foo')
//                      .collection('date').doc('2020-09-03-14')
//                      .collection('metrics').doc('testMetric');
//        await firebase.assertFails(doc.get({ source: 'server' }));
//      });
});