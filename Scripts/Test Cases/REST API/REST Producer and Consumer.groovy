import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import internal.GlobalVariable as GlobalVariable
import com.kms.katalon.core.testobject.ConditionType
import com.kms.katalon.core.testobject.RequestObject
import com.kms.katalon.core.testobject.TestObjectProperty
import com.kms.katalon.core.testobject.impl.HttpTextBodyContent
import com.kms.katalon.core.util.KeywordUtil
import com.kms.katalon.core.webservice.keyword.WSBuiltInKeywords as WS

String apiBaseUrl = System.getenv('API_BASE_URL') ?: GlobalVariable.apiBaseUrl
String eventId = "katalon-${UUID.randomUUID()}"
Map payload = [
    eventId: eventId,
    eventType: 'KATALON_REST_DEMO',
    payload: [message: 'created by Katalon; then read back by Katalon']
]

List<TestObjectProperty> headers = [
    new TestObjectProperty('Accept', ConditionType.EQUALS, 'application/json'),
    new TestObjectProperty('Content-Type', ConditionType.EQUALS, 'application/json')
]

def sendRequest = { String name, String method, String url, String body ->
    RequestObject request = new RequestObject(name)
    request.setServiceType('REST')
    request.setRestUrl(url)
    request.setRestRequestMethod(method)
    request.setHttpHeaderProperties(headers)
    request.setConnectionTimeout(5000)
    request.setSocketTimeout(5000)
    if (body != null) {
        request.setBodyContent(new HttpTextBodyContent(body, 'UTF-8', 'application/json'))
    }
    return WS.sendRequest(request)
}

// Producer: POST creates a resource in the local REST API.
def createResponse = sendRequest(
    'Create demo message',
    'POST',
    "${apiBaseUrl}/api/messages",
    JsonOutput.toJson(payload)
)
WS.verifyResponseStatusCode(createResponse, 201)

Map created = new JsonSlurper().parseText(createResponse.getResponseText()) as Map
assert created.id : 'POST response did not contain a generated resource id.'
assert created.eventId == eventId : 'POST response did not preserve eventId.'

// Consumer: GET reads that resource back and checks the response data.
def readResponse = sendRequest(
    'Read created demo message',
    'GET',
    "${apiBaseUrl}/api/messages/${created.id}",
    null
)
WS.verifyResponseStatusCode(readResponse, 200)

Map readBack = new JsonSlurper().parseText(readResponse.getResponseText()) as Map
assert readBack.id == created.id : 'GET returned a different resource id.'
assert readBack.eventId == eventId : 'GET returned a different eventId.'
assert readBack.eventType == payload.eventType : 'GET returned a different event type.'
assert readBack.payload == payload.payload : 'GET returned a different payload.'

KeywordUtil.logInfo("REST producer/consumer flow passed for eventId=${eventId}")
