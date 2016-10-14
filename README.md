# NiFi Deploy Process Group

This project supports deploying new Process Group into your **LIVE** NiFi data-flow.

Before:
![flow-before](https://raw.githubusercontent.com/ijokarumawak/nifi-deploy-process-group/master/images/flow-before.png)

After executing deploy.js:
![flow-after](https://raw.githubusercontent.com/ijokarumawak/nifi-deploy-process-group/master/images/flow-after.png)


## Restrictions

- Current and target Process Groups must have the identical set of input and output ports
- In a template file, template.name and snippet.processGroups.name should be the same, and follow a naming convention, `<name>:<version>`

## How it works

Unchecked items are planned to be added:

- [ ] Import template
- [ ] Find ProcessGroup by name
- [x] Get upstream connections
- [x] Stop upstream processors
- [ ] Wait until downstream connections get empty (or empty the queue forcefully?)
  - [ ] It can stay as it is if the downstream processor supports multiple version of incomming data
  - [ ] Delete downstream connections (optional)
- [x] Stop current process group
- [x] Switch input to the new process group
- [x] Create a new connection from the new pg to the destination
- [x] Start the new pg
- [x] Start upstream processor

## TODO

- [ ] Externalize NiFiApi module, and embed api functions in it
- [ ] Add more flexibility to command arguments to control behavior

## How to use

```
# Install
$ npm install

# Configure environmental values
$ vi config.yml

# Execute
$ node deploy.js <parentProcessGroupId> <currentProcessGroupId> <targetProcessGroupId>
$ node deploy.js <parentProcessGroupId> <currentProcessGroupId> <templateFilePath>
# E.g.
$ node deploy.js b6a09099-0157-1000-9aa8-fcccef6172ac b7a9cc48-0157-1000-f70f-a7e32aad4bdb b7d14852-0157-1000-0285-d7ee38fb573a
$ node deploy.js b6a09099-0157-1000-9aa8-fcccef6172ac b7a9cc48-0157-1000-f70f-a7e32aad4bdb template.xml


```
