/*
 * Copyright Broker QE authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.brokerqe.claire.perf;

import io.amq.broker.v1beta1.ActiveMQArtemis;
import io.amq.broker.v1beta1.ActiveMQArtemisBuilder;
import io.amq.broker.v1beta1.activemqartemisspec.Acceptors;
import io.brokerqe.claire.AbstractSystemTests;
import io.brokerqe.claire.Constants;
import io.brokerqe.claire.ResourceManager;
import io.brokerqe.claire.TestUtils;
import io.brokerqe.claire.clients.BundledClientDeployment;
import io.brokerqe.claire.clients.DeployableClient;
import io.brokerqe.claire.clients.bundled.ArtemisCommand;
import io.brokerqe.claire.clients.bundled.BundledArtemisClient;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class PerformanceTests extends AbstractSystemTests {

    private static final Logger LOGGER = LoggerFactory.getLogger(PerformanceTests.class);
    private final String testNamespace = getRandomNamespaceName("perf-tests", 3);
    private String testName;
    private String testNameDir;

    @BeforeAll
    void setupClusterOperator() {
        setupDefaultClusterOperator(testNamespace);
    }

    @AfterAll
    void teardownClusterOperator() {
        teardownDefaultClusterOperator(testNamespace);
    }

    @BeforeEach
    void init(TestInfo testInfo) {
        this.testInfo = testInfo;
        testName = testInfo.getTestMethod().orElseThrow().getName().toLowerCase(Locale.ROOT);
        testNameDir = Constants.PERFORMANCE_DIR + "/" + "operator" + "/" + testName;
        TestUtils.createDirectory(testNameDir);
    }

    private ActiveMQArtemis createBrokerDeployment(String name, Acceptors acceptors) {
        ActiveMQArtemis broker = new ActiveMQArtemisBuilder()
                .editOrNewMetadata()
                    .withName(name)
                    .withNamespace(testNamespace)
                .endMetadata()
                .editOrNewSpec()
                    .editOrNewDeploymentPlan()
                        .withSize(1)
                        .withImage("placeholder")
                    .endDeploymentPlan()
                    .withAcceptors(List.of(acceptors))
                .endSpec()
                .build();

        return ResourceManager.createArtemis(testNamespace, broker, true, Constants.DURATION_2_MINUTES);
    }

    private String getAcceptorUrl(String brokerName, String acceptorName) {
        Service acceptorService = getClient().getFirstServiceBrokerAcceptor(testNamespace, brokerName, acceptorName);
        Integer acceptorPort = acceptorService.getSpec().getPorts().get(0).getPort();
        Pod brokerPod = getClient().getFirstPodByPrefixName(testNamespace, brokerName);
        return "tcp://" + brokerPod.getStatus().getPodIP() + ":" + acceptorPort;
    }
    private void startConsumers(DeployableClient<Deployment, Pod> deployableClient, String protocol, String url) {
        Map<String, String> consumerOptions = Map.of(
                "message-count", "5000",
                "protocol", protocol,
                "show-latency", "",
                "consumers", "50",
                "num-connections", "50",
                "url", url,
                "hdr", "consumer.hdr",
                "json", "consumer.json"
        );
        BundledArtemisClient consumer = new BundledArtemisClient(deployableClient, ArtemisCommand.PERF_CONSUMER, consumerOptions);
        consumer.executeCommandInBackground();
    }

    private void startProducer(DeployableClient<Deployment, Pod> deployableClient, String protocol, String url) {
        Map<String, String> producerOptions = Map.of(
                "message-count", "5000",
                "message-size", "1024000",
                "duration", "60",
                "protocol", protocol,
                "show-latency", "",
                "url", url,
                "hdr", "producer.hdr",
                "json", "producer.json"
        );
        BundledArtemisClient producer = new BundledArtemisClient(deployableClient, ArtemisCommand.PERF_PRODUCER, producerOptions);
        producer.executeCommand();
    }

    private void getPerfResults(Pod pod) {
        getClient().copyPodFile(pod, "producer.hdr", Path.of(testNameDir + Constants.FILE_SEPARATOR + "producer.hdr"));
        getClient().copyPodFile(pod,"producer.json", Path.of( testNameDir + Constants.FILE_SEPARATOR + "producer.json"));
        getClient().copyPodFile(pod, "consumer.hdr", Path.of(testNameDir + Constants.FILE_SEPARATOR + "consumer.hdr"));
        getClient().copyPodFile(pod,"consumer.json", Path.of(testNameDir + Constants.FILE_SEPARATOR + "consumer.json"));
    }

    @Test
    void sendReceiveAmqpMessageTest() {
        String acceptorName = "amqp-acceptor";
        String protocol = "amqp";
        Acceptors acceptors = createAcceptor(acceptorName, protocol, 5672);

        String brokerName = "my-broker";
        ActiveMQArtemis broker = createBrokerDeployment(brokerName, acceptors);
        String acceptorUrl = getAcceptorUrl(brokerName, acceptorName);

        Pod brokerPod = getClient().getFirstPodByPrefixName(testNamespace, brokerName);
        DeployableClient<Deployment, Pod> deployableClient = new BundledClientDeployment(testNamespace);
        deployableClient.setContainer(brokerPod);

        startConsumers(deployableClient, protocol, acceptorUrl);

        startProducer(deployableClient, protocol, acceptorUrl);

        getPerfResults(brokerPod);

        ResourceManager.deleteArtemis(testNamespace, broker);
    }

    @Test
    void sendReceiveCoreMessageTest() {
        String acceptorName = "amqp-acceptor";
        String protocol = "CORE";
        Acceptors acceptors = createAcceptor(acceptorName, protocol, 61616);

        String brokerName = "my-broker";
        ActiveMQArtemis broker = createBrokerDeployment(brokerName, acceptors);
        String acceptorUrl = getAcceptorUrl(brokerName, acceptorName);

        Pod brokerPod = getClient().getFirstPodByPrefixName(testNamespace, brokerName);
        DeployableClient<Deployment, Pod> deployableClient = new BundledClientDeployment(testNamespace);
        deployableClient.setContainer(brokerPod);

        startConsumers(deployableClient, protocol, acceptorUrl);

        startProducer(deployableClient, protocol, acceptorUrl);

        getPerfResults(brokerPod);

        ResourceManager.deleteArtemis(testNamespace, broker);
    }
}
