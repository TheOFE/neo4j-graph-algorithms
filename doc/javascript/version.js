window.docMeta = (function () {
  var version = '3.2';
  var name = 'graph-algorithms';
  var href = window.location.href;
  return {
    name: name,
    version: version,
    availableDocVersions: ["3.2", "3.3", "3.4", "3.5-preview"],
    thisPubBaseUri: href.substring(0, href.indexOf(name) + name.length) + '/' + version,
    unversionedDocBaseUri: href.substring(0, href.indexOf(name) + name.length) + '/',
    commonDocsBaseUri: href.substring(0, href.indexOf(name) - 1)
  }
})();

(function () {
  var baseUri = window.docMeta.unversionedDocBaseUri + window.location.pathname.split(window.docMeta.name + '/')[1].split('/')[0] + '/';
  var docPath = window.location.href.replace(baseUri, '');
  window.neo4jPageId = docPath;
})();
// vim: set sw=2 ts=2:
