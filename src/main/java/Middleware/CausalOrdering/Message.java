package Middleware.CausalOrdering;

import io.atomix.utils.serializer.Serializer;

public interface Message {
    Serializer getSerializer();
}
