import enhanced_handler as eh

print("Average rainfall in Maharashtra & Karnataka (last 5 years):")
print(eh.compare_rainfall("Maharashtra", "Karnataka", 5))

print("\nTop 3 crops in Maharashtra of type 'Rice':")
print(eh.top_crops("Maharashtra", "Rice", 5, 3))

print("\nHighest & lowest Rice production in Maharashtra vs Karnataka:")
print(eh.highest_lowest_crop("Maharashtra", "Karnataka", "Rice"))

print("\nCrop trend & rainfall correlation in Western India for Wheat:")
print(eh.crop_trend_with_rainfall("Western India", "Wheat"))
