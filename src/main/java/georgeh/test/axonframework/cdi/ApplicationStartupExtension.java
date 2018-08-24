package georgeh.test.axonframework.cdi;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.spi.AfterBeanDiscovery;
import javax.enterprise.inject.spi.AfterDeploymentValidation;
import javax.enterprise.inject.spi.AnnotatedType;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.CDI;
import javax.enterprise.inject.spi.Extension;
import javax.enterprise.inject.spi.ProcessAnnotatedType;
import javax.enterprise.inject.spi.ProcessProducer;
import javax.enterprise.inject.spi.Producer;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.TypeUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.deltaspike.core.util.HierarchyDiscovery;
import org.apache.deltaspike.core.util.bean.BeanBuilder;
import org.apache.deltaspike.core.util.metadata.builder.ContextualLifecycle;
import org.axonframework.commandhandling.CommandBus;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.commandhandling.model.Repository;
import org.axonframework.config.Configuration;
import org.axonframework.config.Configurer;
import org.axonframework.config.DefaultConfigurer;
import org.axonframework.config.EventHandlingConfiguration;
import org.axonframework.config.SagaConfiguration;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;

public class ApplicationStartupExtension implements Extension {

    private final Logger log = Logger.getLogger(ApplicationStartupExtension.class.getName());

    private Configuration axonConfiguration;

    private List<AnnotatedType<?>> aggregateRootList = new ArrayList<>();

    private List<AnnotatedType<?>> commandHandlerList = new ArrayList<>();

    private List<AnnotatedType<?>> eventHandlerList = new ArrayList<>();

    private List<AnnotatedType<?>> sagaHandlerList = new ArrayList<>();

    private Producer<Configurer> configurerProducer;

    private Producer<EventHandlingConfiguration> eventHandlingConfigurationProducer;

    private List<Pair<Type, Producer<?>>> axonComponentProducers = new ArrayList<>();

    <X> void processAggregateRootType(@Observes final ProcessAnnotatedType<X> pat, final BeanManager bm) {
        AnnotatedType<X> at = pat.getAnnotatedType();
        if (AxonUtils.isAnnotatedAggregateRoot(at.getJavaClass())) {
            log.info("Found aggregate root " + at.getJavaClass());
            aggregateRootList.add(at);
            pat.veto();
        }
    }

    <X> void processCommandHandlerTypes(@Observes final ProcessAnnotatedType<X> pat, final BeanManager bm) {
        AnnotatedType<X> at = pat.getAnnotatedType();
        if (AxonUtils.isCommandHandler(at.getJavaClass()) && !AxonUtils.isAnnotatedAggregateRoot(at.getJavaClass())) {
            log.info("Found command handler " + at.getJavaClass());
            commandHandlerList.add(at);
            // pat.veto();
        }
    }

    <X> void processEventHandlerTypes(@Observes final ProcessAnnotatedType<X> pat, final BeanManager bm) {
        AnnotatedType<X> at = pat.getAnnotatedType();
        if (AxonUtils.isEventHandler(at.getJavaClass()) && !AxonUtils.isAnnotatedAggregateRoot(at.getJavaClass())) {
            log.info("Found event handler " + at.getJavaClass());
            eventHandlerList.add(at);
            // pat.veto();
        }
    }

    <X> void processSagaHandlerTypes(@Observes final ProcessAnnotatedType<X> pat, final BeanManager bm) {
        AnnotatedType<X> at = pat.getAnnotatedType();
        if (AxonUtils.isAnnotatedSaga(at.getJavaClass())) {
            log.info("Found saga handler " + at.getJavaClass());
            sagaHandlerList.add(at);
            // pat.veto();
        }
    }

    <T, X> void processComponentProducers(@Observes final ProcessProducer<T, X> pp, final BeanManager bm) {
        if (pp.getAnnotatedMember().isAnnotationPresent(AxonConfigurationComponent.class)) {
            log.info("Producer for axon component found: " + pp.getProducer());
            axonComponentProducers.add(Pair.of(pp.getAnnotatedMember().getBaseType(), pp.getProducer()));
        }
    }

    <T> void processProducerConfigurer(@Observes final ProcessProducer<T, Configurer> pp, final BeanManager bm) {
        log.info("Producer for Configurer found: " + pp.getProducer());
        this.configurerProducer = pp.getProducer();
    }

    <T> void processProducerEventHandlingConfiguration(@Observes final ProcessProducer<T, EventHandlingConfiguration> pp, final BeanManager bm) {
        log.info("Producer for EventHandlingConfiguration found: " + pp.getProducer());
        this.eventHandlingConfigurationProducer = pp.getProducer();
    }

    void afterBeanDiscovery(@Observes final AfterBeanDiscovery abd, final BeanManager bm) {

        abd.addBean(new BeanWrapper<>(Configuration.class, () -> getAxonConfiguration()));
        abd.addBean(new BeanWrapper<>(CommandGateway.class, () -> getAxonConfiguration().commandGateway()));

        aggregateRootList.stream().map(type -> createRepositoryBean(bm, type)).forEach(abd::addBean);
    }

    private <T> Bean<Repository<T>> createRepositoryBean(final BeanManager bm, final AnnotatedType<T> aggregateType) {
        ContextualLifecycle<Repository<T>> beanLifecycle = new ContextualLifecycle<Repository<T>>() {

            @Override
            public Repository<T> create(final Bean<Repository<T>> bean, final CreationalContext<Repository<T>> creationalContext) {
                Optional<ParameterizedType> repositoryType = bean.getTypes().stream().filter(ParameterizedType.class::isInstance).map(ParameterizedType.class::cast).findFirst();
                if (!repositoryType.isPresent()) {
                	throw new IllegalStateException("Unable to determine repository type from types " + bean.getTypes());
                }
                Type aggregateType = repositoryType.get().getActualTypeArguments()[0];
                @SuppressWarnings("unchecked")
                Class<T> aggregateClass = (Class<T>) aggregateType;
                return getAxonConfiguration().repository(aggregateClass);
            }

            @Override
            public void destroy(final Bean<Repository<T>> bean, final Repository<T> instance, final CreationalContext<Repository<T>> creationalContext) {
                creationalContext.release();
            }
        };

        BeanBuilder<T> aggregateRootBean = new BeanBuilder<T>(bm).readFromType(aggregateType);

        ParameterizedType repositoryType = TypeUtils.parameterize(Repository.class, aggregateType.getBaseType());

        BeanBuilder<Repository<T>> builder = new BeanBuilder<Repository<T>>(bm)
            .beanClass(Repository.class)
            // .qualifiers(CdiUtils.normalizedQualifiers(
            // aggregateRootInfo.getQualifiers(QualifierType.REPOSITORY)))
            .alternative(aggregateRootBean.isAlternative())
            .nullable(aggregateRootBean.isNullable())
            .types(new HierarchyDiscovery(repositoryType).getTypeClosure())
            .scope(aggregateRootBean.getScope())
            .stereotypes(aggregateRootBean.getStereotypes())
            .beanLifecycle(beanLifecycle)
            ;

        if (StringUtils.isNotBlank(aggregateRootBean.getName())) {
            builder.name(aggregateRootBean.getName() + "Repository");
        }
        return builder.create();
    }

    private Configuration getAxonConfiguration() {
        return axonConfiguration;
    }

    void afterDeploymentValidation(@Observes final AfterDeploymentValidation adv, final BeanManager bm) {
        final Configurer configurer;
        if (this.configurerProducer != null) {
            configurer = this.configurerProducer.produce(bm.createCreationalContext(null));
        } else {
            configurer = DefaultConfigurer.defaultConfiguration();
        }

        configurer.configureResourceInjector(c -> new AxonCdiResourceInjector(bm));

        for (Pair<Type, Producer<?>> cp : axonComponentProducers) {
            @SuppressWarnings("unchecked")
            Class<Object> cl = (Class<Object>) cp.getKey();
            if (EventStorageEngine.class.isAssignableFrom(cl)) {
                configurer.configureEmbeddedEventStore(c -> EventStorageEngine.class.cast(cp.getValue().produce(bm.createCreationalContext(null))));
            } else if (CommandBus.class.isAssignableFrom(cl)) {
                configurer.configureCommandBus(c -> CommandBus.class.cast(cp.getValue().produce(bm.createCreationalContext(null))));
            } else {
                configurer.registerComponent(cl, c -> cl.cast(cp.getValue().produce(bm.createCreationalContext(null))));
            }
        }

        // simple saga
        sagaHandlerList.stream().map(AnnotatedType::getJavaClass).map(SagaConfiguration::trackingSagaManager).forEach(configurer::registerModule);

        EventHandlingConfiguration ehConfiguration;
        if (this.eventHandlingConfigurationProducer != null) {
            ehConfiguration = this.eventHandlingConfigurationProducer.produce(bm.createCreationalContext(null));
        } else {
            ehConfiguration = new EventHandlingConfiguration();
        }
        eventHandlerList.stream().map(AnnotatedType::getJavaClass).forEach(c -> ehConfiguration.registerEventHandler(config -> CDI.current().select(c).get()));
        configurer.registerModule(ehConfiguration);

        aggregateRootList.stream().map(AnnotatedType::getJavaClass).forEach(configurer::configureAggregate);

        commandHandlerList.stream().map(AnnotatedType::getJavaClass).forEach(c -> configurer.registerCommandHandler(config -> CDI.current().select(c).get()));

        axonConfiguration = configurer.buildConfiguration();
        axonConfiguration.start();
    }

}
