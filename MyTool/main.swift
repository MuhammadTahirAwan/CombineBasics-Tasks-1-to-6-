import Foundation
import Combine



//Task 1 Create a simple `Just` publisher that emits a single string value (e.g., "Hello, Combine!") and subscribe to it. Print the emitted value in the console.


var cancellables = Set<AnyCancellable>()

let chr_Publisher = Just("Hello, Combine!")

let subscription = chr_Publisher.sink(receiveCompletion: { completion in
        
        print("Stream Completed Successfully \(completion)")
    
    }, receiveValue: { value in
        
        print("Received Value:  \(value)")
        
        
    }
)










//Task 2 Clone previous solution and modify it, using `Future` publisher, print the value after 2 seconds delay.


let delayedPublisher = Future<String, Never> { promise in

    DispatchQueue.main.asyncAfter(deadline: .now() + 2.0) {
        promise(.success("Hello, Combine now with Some Delay!"))
    }
}

let subscription2 = delayedPublisher.sink(receiveCompletion: { completion in
    
        print("2nd Stream Completed Successfully \(completion)")
        
    }, receiveValue: { value in
        
        print("\nReceived Value:  \(value)")
        
        
    }
).store(in: &cancellables)


RunLoop.main.run(until: Date().addingTimeInterval(3.0))







//Task 3 Create 2 publishers: name publisher and surname publisher. Combine the output of 2 publishers this way, that application would print "NAME surname" (name should be capitalized) combination into console. Value transformations should be applied to publishers directly.


let namePublisher = Just("tahir").map { $0.uppercased() }
let surNamePublisher = Just("Awan").map { $0.lowercased() }

let surNameSubscription = namePublisher.combineLatest(surNamePublisher).map { name, surname in
        "\(name) \(surname)"
    }.sink{ value in
        print("\nReceived Value:  \(value)")
    }







//Task 4 Create a PassthroughSubject that emits integers. Use the filter operator to only emit even numbers, then subscribe and print the result.


let passthroughSubject = PassthroughSubject<Int, Never>()


passthroughSubject
    .filter { $0 % 2 == 0}
    .sink(
        receiveCompletion: { completion in
            print("Stream finished: \(completion)")
        },
        receiveValue: { value in
            print("Received: \(value)")
        }
    )
.store(in: &cancellables)


passthroughSubject.send(4)
passthroughSubject.send(3)
passthroughSubject.send(17)












//Task 5 Simulate a text input field by publishing a sequence of strings (e.g., "H", "He", "Hel", "Hello"). Use the `debounce` operator to only emit the final value after a short delay.


let sequenceStrings = ["H", "He", "Hel", "Hello"].publisher

sequenceStrings
    .append(Empty(completeImmediately: false))
    .debounce(for: .milliseconds(500), scheduler: DispatchQueue.main)
    .sink { finalText in
        print("\nSearch query: \(finalText)") // Prints only: "Search query: Hello"
    }
    
    .store(in: &cancellables)

RunLoop.main.run(until: Date().addingTimeInterval(1.5))




//Other Way
// 1. A text field is an ongoing stream of events, so use PassthroughSubject
let textFieldSubject = PassthroughSubject<String, Never>()

// 2. Debounce waits for a 0.5s pause in typing before emitting
textFieldSubject
    .debounce(for: .milliseconds(500), scheduler: DispatchQueue.main)
    .sink { finalText in
        print("\nEmitted value: \(finalText)\n") // Prints only: "Emitted value: Hello"
    }
    .store(in: &cancellables)

// 3. Simulate typing the sequence of strings (each 0.1s apart, faster than the 0.5s debounce)
let inputs = ["H", "He", "Hel", "Hello"]

for (index, text) in inputs.enumerated() {
    DispatchQueue.main.asyncAfter(deadline: .now() + Double(index) * 0.1) {
        textFieldSubject.send(text)
    }
}

// Keep the macOS CLI app running for 1.5s so all keystrokes + the 0.5s debounce complete
RunLoop.main.run(until: Date().addingTimeInterval(1.5))











//Task 6 Create a PassthroughSubject that emits Int values, and use flatMap to convert these into publishers that emit the square of each number.


let newIntSubject = PassthroughSubject<Int, Never>()

newIntSubject
    .flatMap { number in
        Just(number * number)
    }
    .sink(receiveCompletion: { completion in
        print("Stream finished: \(completion)")
    },
        receiveValue: { value in
            print("Received: \(value)")
        }
    )
    .store(in: &cancellables)

let inputsInt = [2,1,56,42]


for i in 0..<inputsInt.count
{
    
    DispatchQueue.main.asyncAfter(deadline: .now() + Double(i) * 0.1) {
        newIntSubject.send(inputsInt[i])
    }
}


RunLoop.main.run(until: Date().addingTimeInterval(3.5))










//Task 7 Create a basic view model with a PassthroughSubject that emits when a button is pressed. Observe the changes and print the button press count.
