package georgeh.test.axonframework.cdi;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.AnnotatedType;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.InjectionTarget;

public class CdiUtils {

    @SuppressWarnings("unchecked")
    public static <T> T injectFields(final BeanManager beanManager, final T instance) {
        if (instance == null) {
            return null;
        }

        CreationalContext<T> creationalContext = beanManager.createCreationalContext(null);

        AnnotatedType<T> annotatedType = beanManager
            .createAnnotatedType((Class<T>) instance.getClass());
        InjectionTarget<T> injectionTarget = beanManager.createInjectionTarget(annotatedType);
        injectionTarget.inject(instance, creationalContext);
        return instance;
    }

}
