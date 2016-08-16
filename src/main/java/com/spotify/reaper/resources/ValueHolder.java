package com.spotify.reaper.resources;

import com.fasterxml.jackson.annotation.JsonProperty;

class ValueHolder<E> {
	@JsonProperty
	E value;
}