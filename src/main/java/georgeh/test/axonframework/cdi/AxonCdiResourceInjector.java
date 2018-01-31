package georgeh.test.axonframework.cdi;

import javax.enterprise.inject.spi.BeanManager;
import org.axonframework.eventhandling.saga.ResourceInjector;

import static java.util.Objects.requireNonNull;

public class AxonCdiResourceInjector implements ResourceInjector {

    private final BeanManager beanManager;

    public AxonCdiResourceInjector(final BeanManager beanManager) {
        this.beanManager = requireNonNull(beanManager);
    }

    @Override
    public void injectResources(final Object saga) {
        CdiUtils.injectFields(beanManager, saga);
    }

}
