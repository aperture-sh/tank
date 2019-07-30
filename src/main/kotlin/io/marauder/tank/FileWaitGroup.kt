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
class FileWaitGroup(private val tyler: Tyler) {
    private var runners = mutableListOf<Job>()
    private var maxRunners = 1
    private var files = mutableListOf<File>()


    suspend fun add(f: File) {
        files.add(f)
        if (runners.isEmpty()) {
            runner@while (true) {
                if (files.isNotEmpty() && runners.size <= maxRunners) {
                    val file = files.first()
                    files.remove(file)
                    val job = GlobalScope.launch {
                        log.info("Start processing file: ${file.name}")

                        file.bufferedReader().useLines { lines ->
                            var count = 0
                            lines.forEach { line ->
                                val feature = JSON.plain.parse(Feature.serializer(), line)
                                tyler.import(feature)
                                count += 1
                                if (count % 1000 == 0) log.info("1000 features imported")
                            }
                        }

                    }
                    runners.add(job)
                    GlobalScope.launch {
                        job.join()
                        log.info("Finished processing file: ${file.name}")
                        runners.remove(job)
                        file.delete()
                    }
                } else {
                    log.info("Waiting for free runners to procress more files")
                    delay(2000)
                    if (files.isEmpty()) break@runner

                }
            }
        }


    }

    companion object {
        private val log = LoggerFactory.getLogger(FileWaitGroup::class.java)
    }
}