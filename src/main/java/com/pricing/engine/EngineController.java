package com.pricing.engine;

import com.pricing.engine.generated.api.PricingApi;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class EngineController implements PricingApi {

	private final EngineService pricingService;

	public EngineController(EngineService pricingService) {
		this.pricingService = pricingService;
	}

	@Override
	public ResponseEntity<Void> wakeUp() {
		pricingService.wakeUp();
		return ResponseEntity.accepted().build();
	}
}
