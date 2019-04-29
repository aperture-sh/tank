import com.datastax.driver.core.Cluster
import io.marauder.supercharged.Clipper
import io.marauder.supercharged.Projector
import io.marauder.supercharged.models.Feature
import io.marauder.supercharged.models.GeoJSON
import io.marauder.tank.Tyler
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.json.JSON
import kotlinx.serialization.parse
import java.io.File

@ImplicitReflectionSerializer
class ClipperTest {

    private val clipper = Clipper()

    private fun readFeature(type: String, fileName: String) =
            JSON.plain.parse<Feature>(javaClass.getResource("fixtures/$type/$fileName.json").readText())

//    @Test
    fun test1() {
        val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
        val session = cluster.connect("geo")


        val tiler = Tyler(session)
        val projector = Projector()
//        val geojson = JSON.plain.parse<GeoJSON>(File("/Volumes/Samsung_T5/data/np/10-000.csv.json").readText())
        val geojson = JSON.plain.parse<GeoJSON>(File("Alaska.geojson").readText())

            tiler.tiler(projector.projectFeatures(geojson))

    }
}