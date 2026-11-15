package com.hz.patterns.locality;

import java.io.Serializable;
import java.util.UUID;

public record Reservation(boolean accepted, Integer before, Integer after,
                          UUID executedOn) implements Serializable {
}
