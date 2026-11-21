package com.hz.patterns.eventualjoins;

import java.io.Serializable;

record JoinResult(String productName, int attempts) implements Serializable {
}
