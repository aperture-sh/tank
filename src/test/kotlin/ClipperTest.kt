import com.datastax.driver.core.Cluster
import io.ktor.application.call
import io.ktor.request.receiveText
import io.marauder.supercharged.Clipper
import io.marauder.supercharged.Projector
import io.marauder.supercharged.models.Feature
import io.marauder.supercharged.models.GeoJSON
import io.marauder.supercharged.models.Geometry
import io.marauder.tank.Tiler
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.json.JSON
import kotlinx.serialization.parse
import org.junit.Test
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


        val tiler = Tiler(session)
        val projector = Projector()
//        val geojson = JSON.plain.parse<GeoJSON>(File("/Volumes/Samsung_T5/data/np/10-000.csv.json").readText())
        val geojson = JSON.plain.parse<GeoJSON>(File("Alaska.geojson").readText())

            tiler.tiler(projector.projectFeatures(geojson))

    }
}