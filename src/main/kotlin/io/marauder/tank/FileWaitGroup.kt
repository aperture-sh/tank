package io.marauder.tank

import io.marauder.charged.models.Feature
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.json.JSON
import org.slf4j.LoggerFactory
import java.io.File

@ImplicitReflectionSerializer
class FileWaitGroup(private val tyler: Tyler, private val tmpDir : String) {
    private val maxRunners = 1
    private var runners = 0

    suspend fun startRunner() {
        if (runners < maxRunners) {
            runner@while (true) {
                val fileList = File(tmpDir).listFiles { _, name -> !name.contains(".lock")}
                if (fileList != null && fileList.isNotEmpty() && runners < maxRunners) {
                    runners += 1
                    val file = fileList.first()
                    val tmpFile = File("${file.absolutePath}.lock")
                    file.renameTo(tmpFile)
                    val job = GlobalScope.launch {
                        log.info("Start processing file: ${file.name}")
                        var count = 0

                        tmpFile.bufferedReader().forEachLine { line ->
                            val feature = JSON.plain.parse(Feature.serializer(), line)
                            tyler.import(feature)
                            count += 1
                            if (count % 1000 == 0) log.info("1000 features imported")
                        }

                    }
                    GlobalScope.launch {
                        job.join()
                        log.info("Finished processing file: ${file.name}")
                        tmpFile.delete()
                        runners -= 1
                    }
                } else {
                    delay(2000)
                    val fileList2 = File(tmpDir).listFiles { _, name -> !name.contains(".lock")}
                    if (fileList2 != null && fileList2.isEmpty()) {
                        break@runner
                    }

                }
            }
        }


    }

    companion object {
        private val log = LoggerFactory.getLogger(FileWaitGroup::class.java)
    }
}