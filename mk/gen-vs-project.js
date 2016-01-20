# This script builds RethinkDB.vcxproj
# From the template in mk/RethinkDB.vcxproj.xsl
# Using the settings in Rethinkdb.vcxproj.xml

function load(path) {
    var xml = new ActiveXObject("Msxml2.DOMDocument.6.0");
    if (xml.load(path)) {
        return xml;
    }
    WScript.Echo("Parse error in '" + path + "': " + xml.parseError.reason);
    WScript.Quit()
}

xml = load("RethinkDB.vcxproj.xml");
xsl = load("mk/RethinkDB.vcxproj.xsl");

out = new ActiveXObject("Msxml2.DOMDocument.6.0");
xml.transformNodeToObject(xsl, out);

out.save("RethinkDB.vcxproj");
