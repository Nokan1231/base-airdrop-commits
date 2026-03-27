✓○✓✓✓



// Input validation for add command
function validateTaskTitle(title) {
    if (!title || title.trim().length === 0) {
          console.error('Error: Task title cannot be empty!');
          process.exit(1);
    }
    return title.trim();
}
