# Firebase Config

Install the [Firebase CLI](https://firebase.google.com/docs/cli) and login as
follows:

```shell script
firebase login
```

## Firestore Security Rules

This provides a sample way to configure Firestore documents for
Exposure Notifications Private Analytics.

### Testing

First install the emulator, npm testing library and jest:

```shell script
firebase setup:emulators:firestore
npm init -y
npm i @firebase/testing
npm i jest
```

Then start the emulator and execute the test script:

```shell script
firebase emulators:exec --only firestore "npm run test"
```

### Deploying

You can update your projects Firestore Security Policy with these rules as
follows:

```shell script
firebase deploy --only firestore:rules
```
