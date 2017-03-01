package org.claz.mongoals

import com.mongodb.BasicDBObject
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.commons.io.IOUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Required
import org.springframework.core.io.Resource
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Query

import javax.annotation.PostConstruct

import static org.springframework.util.DigestUtils.md5Digest

@CompileStatic
@Slf4j
class Goal implements Runnable {

    @Autowired(required = false)
    private MongoTemplate mongo
    private Resource[] scripts
    private String schema = "_version"

    void setMongo(MongoTemplate mongo) {
        this.mongo = mongo
    }

    @Required
    void setScripts(Resource[] resources) {
        this.scripts = resources
    }

    void setSchema(String schema) {
        this.schema = schema
    }

    @PostConstruct
    void run() {
        log.info("Updating MongoDB Schema...")
        createCollectionIfNeeded()
        List<Version> versions = getVersions()

        scripts.sort { r1, r2 -> r1.filename <=> r2.filename } //sort alphabetically
                .findAll { resource -> !versions?.find { it.fileName == resource.filename } }   //version has not been applied yet
                .each { log.info "New Script: ${it.filename}" }
                .each(this.&runScript)
    }

    private void createCollectionIfNeeded() {
        log.info("Schema collection does not exist. Creating $schema...")
        if (!mongo.collectionExists(schema)) {
            mongo.createCollection(schema)
        }
    }

    private List<Version> getVersions() {
        return mongo.find(new Query(), Version, schema)
    }

    private void runScript(Resource resource) {
        resource.inputStream.withCloseable { stream ->
            log.info "Applying Update Script: ${resource.filename}..."
            String content = IOUtils.toString(stream)
            String md5 = md5Digest(content.bytes)

            def command = new BasicDBObject([eval: content])
            try {
                mongo.executeCommand(command).throwOnError()
                Version version = new Version(fileName: resource.filename, md5: md5)
                mongo.save(version, schema)
            } catch (Exception ex) {
                throw new IllegalArgumentException("Error executing ${resource.filename}", ex)
            }
        }
    }


}
