# Database Synchronization Mini-Project

**Advanced Operating Systems - Concurrent Programming**

📚 **Repository**: [https://github.com/halladj/advanced-os-miniProject](https://github.com/halladj/advanced-os-miniProject)

## ⚠️ WARNING: This Code is Intentionally Broken!

This repository contains an **unsynchronized** implementation of an in-memory database that demonstrates race conditions. Your task is to fix these concurrency bugs by implementing proper synchronization.

## 🎯 Project Overview

You will implement synchronization mechanisms to fix race conditions in a concurrent database system. This project teaches:

- Race condition identification and debugging
- Mutex and monitor patterns
- Reader-writer locks
- Channel-based synchronization
- Performance analysis of concurrent systems

## 📋 What's Included

This starter repository contains:

- `database.go` - Unsynchronized database implementation (UNSAFE!)
- `client.go` - Test scenarios demonstrating race conditions
- `main.go` - Entry point to run demonstrations
- `go.mod` - Go module definition
- `problem_statement.pdf` - Complete project requirements and grading rubric

## 🚀 Getting Started

### Prerequisites

- Go 1.21 or later
- Basic understanding of concurrency concepts

### Running the Demonstration

See the race conditions in action:

```bash
# Run without race detector (will show incorrect results)
go run .

# Run with race detector (will show data races)
go run -race .
```

### Expected Behavior (Unsynchronized Version)

When you run this code, you will see:

1. **Counter Scenario**: Lost updates (final value < expected)
2. **Bank Transfer**: Money disappearing (total < 2000)
3. **Read-Write**: Inconsistent reads detected

**This is intentional!** These bugs demonstrate why synchronization is needed.

## 📊 Example Output

```
=== Counter Increment Scenario ===
Expected final value: 1000
Final counter value: 743
❌ RACE CONDITION DETECTED! Lost 257 updates (25.7% lost)

=== Bank Transfer Scenario ===
Initial state: account_A=1000, account_B=1000, total=2000
Final state: account_A=650, account_B=1180, total=1830
❌ RACE CONDITION DETECTED! Lost $170 in total
```

## 🔍 Understanding the Code

### Key Files

#### `database.go`
Contains the database implementation with **NO synchronization**. Every unsafe operation is marked with `// UNSAFE:` comments.

**Key race conditions to find:**
- Concurrent map access
- Non-atomic read-modify-write operations
- Unsynchronized statistics updates
- Concurrent transaction counter increments

#### `client.go`
Simulates concurrent clients with three scenarios:
1. **Counter Increment** - Classic lost update problem
2. **Bank Transfer** - Money disappearing due to races
3. **Read-Write Consistency** - Dirty reads and torn writes

#### `main.go`
Runs all demonstration scenarios and reports results.

## 📝 Your Task

### Requirements

1. **Implement Synchronization** (in a new `solution/` directory):
   - Implement at least 2 synchronization approaches:
     - Mutex-based (required)
     - Monitor pattern, RWLock, or Channel-based
   
2. **Pass All Tests**:
   - All provided tests must pass
   - No race conditions with `go test -race`
   
3. **Write Analysis Report** (2-3 pages):
   - Identify 3+ race conditions in this code
   - Explain your synchronization strategy
   - Compare performance of different approaches

### Suggested Workflow

1. **Observe**: Run this code with `-race` flag
2. **Analyze**: Identify where and why races occur
3. **Design**: Plan your synchronization strategy
4. **Implement**: Create synchronized version
5. **Test**: Verify correctness with `go test -race`
6. **Benchmark**: Compare performance
7. **Document**: Write your analysis

## 🛠️ Development Tips

### Using the Race Detector

The race detector is your best friend:

```bash
# Run with race detection
go run -race .

# Test with race detection
go test -race -v

# Build with race detection
go build -race
```

### Common Race Conditions to Look For

- **Lost Updates**: Read-modify-write without atomicity
- **Dirty Reads**: Reading while another goroutine is writing
- **Inconsistent Reads**: Reading multiple related values non-atomically
- **Map Races**: Concurrent map access (reads and writes)

## 📚 Resources

### Go Concurrency

- [Effective Go - Concurrency](https://go.dev/doc/effective_go#concurrency)
- [Go Race Detector](https://go.dev/doc/articles/race_detector)
- [Sync Package Documentation](https://pkg.go.dev/sync)

### Recommended Reading

- *The Little Book of Semaphores* by Allen Downey
- *Operating Systems: Three Easy Pieces* - Concurrency chapters
- Go Blog: [Share Memory By Communicating](https://go.dev/blog/codelab-share)

## 🎓 Learning Objectives

By completing this project, you will:

- ✅ Understand how race conditions occur
- ✅ Learn to use Go's race detector
- ✅ Implement multiple synchronization patterns
- ✅ Analyze performance trade-offs
- ✅ Write concurrent code that is correct and efficient

## ⚡ Quick Commands

```bash
# See race conditions
go run -race .

# Build (will compile but has races!)
go build

# Format code
go fmt ./...

# Check for common mistakes
go vet ./...
```

## 🤝 Team Collaboration

This is a **group project for 2 students**. Suggested division of work:

- **Student 1**: Implement mutex and monitor approaches
- **Student 2**: Implement RWLock and/or channel approaches
- **Together**: Code review, testing, and report writing

Use Git for collaboration:
```bash
git init
git add .
git commit -m "Initial commit - unsynchronized version"
git branch student1-mutex
git branch student2-rwlock
```

## �� Project Structure

```
database-sync-project/
├── unsynchronized/          # This starter code (UNSAFE)
│   ├── database.go
│   ├── client.go
│   ├── main.go
│   ├── go.mod
│   ├── problem_statement.pdf
│   └── README.md           # This file
│
└── solution/               # Your code goes here
    ├── database_mutex.go   # Your implementation
    ├── database_*.go       # Other approaches
    ├── *_test.go          # Tests (will be provided)
    └── go.mod
```

## ❓ FAQ

**Q: Why does the counter value change each time I run it?**  
A: That's the race condition! The exact timing of goroutine execution is non-deterministic.

**Q: Should I modify the files in this directory?**  
A: No, create your solution in a new `solution/` directory. Keep this code as reference.

**Q: How do I know if my solution is correct?**  
A: Run `go test -race -v` - it should pass with zero race warnings.

**Q: Which synchronization approach should I use?**  
A: You must implement at least 2. Start with mutex (simplest), then try others.

## 🎯 Success Criteria

Your solution is complete when:

- ✅ All tests pass without race conditions
- ✅ `go test -race` shows no warnings
- ✅ Counter scenario: final value = expected
- ✅ Bank transfer: total money preserved
- ✅ Read-write: no inconsistent reads
- ✅ Code is well-documented
- ✅ Analysis report completed

## 🐛 Known Issues (Intentional!)

This code has the following **intentional** bugs:

- No synchronization on map access
- Race conditions in transaction counter
- Unsynchronized statistics updates
- Non-atomic read-modify-write operations
- Concurrent iteration over shared map

**Your job is to fix them!**

## 📞 Getting Help

If you're stuck:

1. Review the `// UNSAFE:` comments in the code
2. Run with `-race` to see exact race locations
3. Consult the `problem_statement.pdf` for detailed requirements
4. Discuss concepts (not code) with other groups
5. Ask your instructor during office hours

---

**Good luck! Understanding concurrency is one of the most valuable skills in systems programming.**

*Remember: The goal isn't just to make tests pass, but to understand WHY your solution works.*
